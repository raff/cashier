package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/raff/cashier/storage"
)

type Cashier struct {
	sdb *storage.StorageDB
}

type mmap = map[string]interface{}

func errorMessage(code, subcode interface{}, info mmap) mmap {
	message := mmap{"code": code, "subcode": subcode}

	for k, v := range info {
		message[k] = v
	}

	return message
}

func (cc *Cashier) putEntry(c echo.Context) error {
	id := c.Param("id")
	resumePos := int64(0)

	info, err := cc.sdb.Stat(id)
	if err == nil { // already exists
		if srange := c.Request().Header.Get("Content-Range"); srange != "" {
			var start, stop, length int64
			if _, err := fmt.Sscanf(srange, "bytes %d-%d/%d", &start, &stop, &length); err != nil {
				return c.String(http.StatusInternalServerError, err.Error())
			}

			if start != info.Next || length != info.Length {
				c.Logger().Debugf("upload %v: range %v-%v/%v next %v/%v",
					id, start, stop, length, info.Next, info.Length)
				return c.String(http.StatusBadRequest, "invalid-range")
			}
			if stop < length-1 && (stop-start+1)%storage.BlockSize != 0 {
				c.Logger().Debugf("upload %v: range %v-%v/%v next %v/%v",
					id, start, stop, length, info.Next, info.Length)
				return c.String(http.StatusBadRequest, "invalid-range")
			}

			resumePos = start
			c.Logger().Debugf("upload %v: resume from %v", id, resumePos)
		} else if info.Next != storage.FileComplete {
			c.Response().Header().Set("Range",
				fmt.Sprintf("bytes=%v-%v/%v", info.Next, info.Length-1, info.Length))
			return c.JSON(http.StatusConflict, errorMessage("conflict", "incomplete", mmap{
				"resume-from": info.Next,
			}))
		} else {
			return c.JSON(http.StatusConflict, errorMessage("conflict", "already-exists", nil))
		}
	} else if err != storage.ErrNotFound {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	var reader io.Reader
	var size int64

	finfo, err := c.FormFile("file")
	if err == nil {
		ffile, ferr := finfo.Open()
		if ferr != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		defer ffile.Close()

		if resumePos == 0 {
			err = cc.sdb.CreateFile(id, finfo.Filename, finfo.Header.Get("Content-Type"), finfo.Size, nil)
			c.Logger().Debugf("upload %v: created", id)
		}
		reader = ffile
		size = finfo.Size
	} else if err == http.ErrNotMultipart {
		err = nil

		// not a form, we just read the body
		if resumePos == 0 {
			err = cc.sdb.CreateFile(id, id, c.Request().Header.Get("Content-Type"), c.Request().ContentLength, nil)
			c.Logger().Debugf("upload %v: created", id)
		}
		reader = c.Request().Body
		size = c.Request().ContentLength
	}
	if err == storage.ErrExists {
		c.Logger().Errorf("upload %v: %v", id, err.Error())
		return c.String(http.StatusConflict, "ALREADY EXISTS")
	}
	if err != nil {
		c.Logger().Errorf("upload %v: %v", id, err.Error())
		return c.String(http.StatusInternalServerError, err.Error())
	}

	var buf = make([]byte, storage.BlockSize)
	var pos int64
	var nread int64

	for pos = resumePos; pos != storage.FileComplete; {
		var n int

		n, err = reader.Read(buf)
		if err == io.EOF {
			if n == 0 {
				break
			}
		} else if err != nil {
			c.Logger().Warnf("upload %v: error reading %v", id, err)
			break
		}

		c.Logger().Debugf("upload %v: read %v", id, n)

		npos, err := cc.sdb.WriteAt(id, pos, buf[:n])
		if err != nil {
			c.Logger().Warnf("upload %v: error writing %v", id, err)
			break
		}

		c.Logger().Debugf("upload %v: wrote %v, next %v", id, n, npos)
		nread += int64(n)
		pos = npos
	}
	if nread != size {
		c.Logger().Errorf("upload %v: expected %v read %v writepos %v", id, size, nread, pos)
	}
	if err != nil {
		c.Logger().Errorf("upload %v: %v", id, err.Error())
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.String(http.StatusCreated, "CREATED")
}

func (cc *Cashier) deleteEntry(c echo.Context) error {
	id := c.Param("id")
	if err := cc.sdb.DeleteFile(id); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.String(http.StatusNoContent, "DELETED")
}

func (cc *Cashier) getMetadata(c echo.Context) error {
	id := c.Param("id")
	info, err := cc.sdb.Stat(id)
	if err == storage.ErrNotFound {
		return c.String(http.StatusNotFound, "NOT FOUND")
	}
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, info)
}

type ReadSeeker struct {
	sdb    *storage.StorageDB
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
		return c.String(http.StatusNotFound, "NOT FOUND")
	}
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	if info.ContentType != "" {
		c.Response().Header().Set("Content-Type", info.ContentType)

	}

	if info.Next != storage.FileComplete {
		c.Response().Header().Set("X-Current-Length", strconv.Itoa(int(info.Next)))
		c.Response().Header().Set("X-Total-Length", strconv.Itoa(int(info.Length)))
		return c.String(http.StatusForbidden, "INCOMPLETE")
	}

	if info.Hash != "" {
		c.Response().Header().Set("ETag", strconv.Quote(info.Hash))
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

	sdb, err := storage.Open(*path, false, *ttl)
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

	e.POST("/x/:id", cashier.putEntry).Name = "Create"
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
