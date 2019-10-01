package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/raff/cashier/storage"
)

type Cashier struct {
	sdb storage.StorageDB
}

type mmap = map[string]interface{}

func statusMessage(code, subcode interface{}, info mmap) mmap {
	message := mmap{"code": code, "subcode": subcode}

	for k, v := range info {
		message[k] = v
	}

	return message
}

func (cc *Cashier) createEntry(c echo.Context) error {
	id := c.Param("id")

	log.Println("create", id)

	var reader io.Reader

	size := int64(-1)

	if c.Request().Header.Get("X-File-Length") != "" {
		fmt.Sscanf(c.Request().Header.Get("X-File-Length"), "%d", &size)
	}

	mp, err := c.Request().MultipartReader()
	if err == http.ErrNotMultipart {
		err = nil

		fname := id
		cdisp := c.Request().Header.Get("Content-Disposition")
		if cdisp != "" {
			_, params, _ := mime.ParseMediaType(cdisp)
			if _, ok := params["filename"]; ok {
				fname = params["filename"]
			}
		}

		if size < 0 {
			size = c.Request().ContentLength
		}

		// not a form, we just read the body
		err = cc.sdb.CreateFile(id, fname, c.Request().Header.Get("Content-Type"), size, nil)
		reader = c.Request().Body
	} else if err == nil {
		fname := id
		ftype := ""

		for {
			p, err := mp.NextPart()
			if err != nil {
				return c.JSON(http.StatusInternalServerError, statusMessage("error", err.Error(), nil))
			}

			if p.FormName() == "file" { // file to upload
				if p.FileName() != "" {
					fname = p.FileName()
				}

				reader = p
				ftype = p.Header.Get("Content-Type")

				if p.Header.Get("Content-Length") != "" {
					fmt.Sscanf(p.Header.Get("Content-File-Length"), "%d", &size)
				}

				// this seems to casue the server to read the full request
				// before returning an error
				// defer p.Close()
				break
			}
		}

		if reader == nil {
			return c.JSON(http.StatusBadRequest, statusMessage("missing", "missing-file", nil))
		}
		if size < 0 {
			return c.JSON(http.StatusBadRequest, statusMessage("missing", "missing-file-length", nil))
		}

		err = cc.sdb.CreateFile(id, fname, ftype, size, nil)
	} else {
		log.Printf("upload %v: cannot get form data - %v", id, err)
	}

	if err == storage.ErrExists {
		log.Printf("upload %v: exists", id)

		info, _ := cc.sdb.Stat(id)
		if info != nil && info.Next != storage.FileComplete {
			c.Response().Header().Set("Range",
				fmt.Sprintf("bytes=%v-%v/%v", info.Next, info.Length-1, info.Length))
		}
		return c.JSON(http.StatusConflict, statusMessage("conflict", "file-exists", nil))
	}
	if err != nil {
		log.Printf("upload %v: %v", id, err.Error())
		return c.JSON(http.StatusInternalServerError, statusMessage("error", err.Error(), nil))
	}

	log.Printf("upload %v: created", id)

	var buf = make([]byte, storage.BlockSize)
	var pos int64
	var nread int64

	for pos != storage.FileComplete {
		var n int

		n, err = io.ReadAtLeast(reader, buf, storage.BlockSize)
		if err == io.EOF {
			if n == 0 {
				break
			}
		} else if err != nil {
			log.Printf("upload %v: error reading - %v", id, err)
			break
		}

		log.Printf("upload %v: read %v", id, n)

		npos, err := cc.sdb.WriteAt(id, pos, buf[:n])
		if err != nil {
			log.Printf("upload %v: error writing - %v", id, err)
			break
		}

		log.Printf("upload %v: wrote %v, next %v", id, n, npos)
		nread += int64(n)
		pos = npos
	}
	if nread != size {
		log.Printf("upload %v: expected %v read %v writepos %v", id, size, nread, pos)
	}
	if err != nil {
		log.Printf("upload %v: %v", id, err.Error())
		return c.JSON(http.StatusInternalServerError, statusMessage("error", err.Error(), nil))
	}

	return c.JSON(http.StatusCreated, statusMessage("success", "created", nil))
}

func (cc *Cashier) updateEntry(c echo.Context) error {
	id := c.Param("id")

	info, err := cc.sdb.Stat(id)
	if err == storage.ErrNotFound {
		return c.JSON(http.StatusNotFound, statusMessage("missing", "not-found", nil))
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, statusMessage("error", err.Error(), nil))
	}
	if info.Next == storage.FileComplete {
		return c.JSON(http.StatusConflict, statusMessage("conflict", "complete", nil))
	}

	srange := c.Request().Header.Get("Content-Range")
	if srange == "" {
		c.Response().Header().Set("Range",
			fmt.Sprintf("bytes=%v-%v/%v", info.Next, info.Length-1, info.Length))
		return c.JSON(http.StatusBadRequest, statusMessage("missing", "range-expected", nil))
	}

	var start, stop, length int64
	if _, err := fmt.Sscanf(srange, "bytes %d-%d/%d", &start, &stop, &length); err != nil {
		c.Response().Header().Set("Range",
			fmt.Sprintf("bytes=%v-%v/%v", info.Next, info.Length-1, info.Length))
		return c.JSON(http.StatusBadRequest, statusMessage("invalid", "invalid-range", nil))
	}
	if start != info.Next || length != info.Length {
		log.Printf("upload %v: range %v-%v/%v next %v/%v",
			id, start, stop, length, info.Next, info.Length)
		c.Response().Header().Set("Range",
			fmt.Sprintf("bytes=%v-%v/%v", info.Next, info.Length-1, info.Length))
		return c.JSON(http.StatusBadRequest, statusMessage("invalid", "invalid-range", nil))
	}
	if stop < length-1 && (stop-start+1)%storage.BlockSize != 0 {
		log.Printf("upload %v: range %v-%v/%v next %v/%v",
			id, start, stop, length, info.Next, info.Length)
		c.Response().Header().Set("Range",
			fmt.Sprintf("bytes=%v-%v/%v", info.Next, info.Length-1, info.Length))
		return c.JSON(http.StatusBadRequest, statusMessage("invalid", "invalid-range", nil))
	}

	log.Printf("upload %v: resume from %v", id, start)

	reader := c.Request().Body
	size := c.Request().ContentLength

	buf := make([]byte, storage.BlockSize)
	pos := int64(0)
	nread := int64(0)

	for pos = start; pos != storage.FileComplete; {
		var n int

		n, err = io.ReadAtLeast(reader, buf, storage.BlockSize)
		if err == io.EOF {
			if n == 0 {
				break
			}
		} else if err != nil {
			log.Printf("upload %v: error reading %v", id, err)
			break
		}

		log.Printf("upload %v: read %v", id, n)

		npos, err := cc.sdb.WriteAt(id, pos, buf[:n])
		if err != nil {
			log.Printf("upload %v: error writing %v", id, err)
			break
		}

		log.Printf("upload %v: wrote %v, next %v", id, n, npos)
		nread += int64(n)
		pos = npos
	}
	if nread != size {
		log.Printf("upload %v: expected %v read %v writepos %v", id, size, nread, pos)
	}
	if err != nil {
		log.Printf("upload %v: %v", id, err.Error())
		return c.JSON(http.StatusInternalServerError, statusMessage("error", err.Error(), nil))
	}

	return c.JSON(http.StatusCreated, statusMessage("success", "updated", nil))
}

func (cc *Cashier) deleteEntry(c echo.Context) error {
	id := c.Param("id")
	if err := cc.sdb.DeleteFile(id); err != nil {
		return c.JSON(http.StatusInternalServerError, statusMessage("error", err.Error(), nil))
	}

	return c.JSON(http.StatusCreated, statusMessage("success", "deleted", nil))
}

func (cc *Cashier) getMetadata(c echo.Context) error {
	id := c.Param("id")
	info, err := cc.sdb.Stat(id)
	if err == storage.ErrNotFound {
		return c.JSON(http.StatusNotFound, statusMessage("missing", "not-found", nil))
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, statusMessage("error", err.Error(), nil))
	}

	return c.JSON(http.StatusOK, info)
}

type ReadSeeker struct {
	sdb    storage.StorageDB
	key    string
	pos    int64
	length int64
}

func (rs *ReadSeeker) Read(p []byte) (int, error) {
	n, err := rs.sdb.ReadAt(rs.key, p, rs.pos)
	rs.pos += n

	if err != nil {
		log.Println("Read", rs.key, rs.pos, err)
	}

	return int(n), err
}

var errWhence = errors.New("Seek: invalid whence")
var errOffset = errors.New("Seek: invalid offset")

func (rs *ReadSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	default:
		return 0, errWhence

	case io.SeekStart:
		// offset is already correct
		break

	case io.SeekCurrent:
		offset += rs.pos

	case io.SeekEnd:
		offset = rs.length + offset
	}

	if offset < 0 {
		return 0, errOffset
	}

	rs.pos = offset
	return offset, nil
}

func (cc *Cashier) getEntry(c echo.Context) error {
	id := c.Param("id")
	info, err := cc.sdb.Stat(id)
	if err == storage.ErrNotFound {
		return c.JSON(http.StatusNotFound, statusMessage("missing", "not-found", nil))
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, statusMessage("error", err.Error(), nil))
	}

	if info.ContentType != "" {
		c.Response().Header().Set("Content-Type", info.ContentType)
	}
	if info.Next != storage.FileComplete {
		c.Response().Header().Set("Range",
			fmt.Sprintf("bytes=%v-%v/%v", info.Next, info.Length-1, info.Length))
		return c.JSON(http.StatusForbidden, statusMessage("not-ready", "incomplete", nil))
	}
	if info.Hash != "" {
		c.Response().Header().Set("ETag", fmt.Sprintf("%q", info.Hash))
	}

	http.ServeContent(c.Response(), c.Request(), info.Name, info.Created, &ReadSeeker{sdb: cc.sdb, key: id, pos: 0, length: info.Length})
	return nil
}

func main() {
	path := flag.String("path", "storage.data", "path to data folder")
	ttl := flag.Duration("ttl", 10*time.Minute, "time to live")
	debug := flag.Bool("debug", false, "debug logging")
	//gc := flag.Bool("gc", false, "run value-log gc")

	flag.Parse()

	sdb, err := storage.OpenBadger(*path, false, *ttl)
	if err != nil {
		log.Fatal(err)
	}

	defer sdb.Close()

	// Echo instance
	e := echo.New()
	e.Debug = *debug
	cashier := &Cashier{sdb: sdb}

	// Middleware
	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "time=${time_rfc3339} method=${method}, uri=${uri}, status=${status} in:${bytes_in} out:${bytes_out} elapsed:${latency_human}\n"}))
	e.Use(middleware.Recover())

	// Routes
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	}).Name = "Ping"

	e.GET("/routes", func(c echo.Context) error {
		return c.JSON(http.StatusOK, e.Routes())
	}).Name = "Routes"

	e.POST("/x/:id", cashier.createEntry).Name = "Create"
	e.PUT("/x/:id", cashier.updateEntry).Name = "Update"
	e.DELETE("/x/:id", cashier.deleteEntry).Name = "Delete"
	e.GET("/x/:id", cashier.getEntry).Name = "Get"
	e.HEAD("/x/:id", cashier.getEntry).Name = "Head"
	e.GET("/x/:id/meta", cashier.getMetadata).Name = "Get Metadata"

	go func() {
		// Start server
		if err := e.Start(":1999"); err != nil && err != http.ErrServerClosed {
			e.Logger.Error("Server didn't start - ", err)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)

	<-quit
	log.Println("Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		e.Logger.Fatal(err)
	}
}
