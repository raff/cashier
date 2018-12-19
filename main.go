package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/raff/cashier/storage"
)

type Cashier struct {
	sdb *storage.StorageDB
}

func (cc *Cashier) putEntry(c echo.Context) error {
	id := c.Param("id")

	var reader io.Reader
	var size int64

	finfo, err := c.FormFile("file")
	if err == nil {
		ffile, ferr := finfo.Open()
		if ferr != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		defer ffile.Close()

		err = cc.sdb.CreateFile(id, finfo.Filename, finfo.Header.Get("Content-Type"), finfo.Size, nil)
		reader = ffile
		size = finfo.Size
	} else if err == http.ErrNotMultipart {
		// not a form, we just read the body
		err = cc.sdb.CreateFile(id, id, c.Request().Header.Get("Content-Type"), c.Request().ContentLength, nil)
		reader = c.Request().Body
		size = c.Request().ContentLength
	}

	if err == storage.ErrExists {
		return c.JSON(http.StatusConflict, "ALREADY EXISTS")
	}
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	var buf = make([]byte, storage.BlockSize)
	var nread int64

	for pos := int64(0); pos != storage.FileComplete; {
		var n int

		n, err = reader.Read(buf)
		if err == io.EOF {
			if n == 0 {
				break
			}
		} else if err != nil {
			break
		}

		npos, err := cc.sdb.WriteAt(id, pos, buf[:n])
		if err != nil {
			break
		}

		nread += int64(n)
		pos = npos
	}

	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	if nread != size {
		c.Logger().Error("expected", size, "read", nread)
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

	http.ServeContent(c.Response(), c.Request(), info.Name, info.Created, &ReadSeeker{sdb: cc.sdb, key: id, pos: 0, length: info.Length})
	return nil
}

func main() {
	path := flag.String("path", "storage.data", "path to data folder")
	ttl := flag.Duration("ttl", 10*time.Minute, "time to live")
	//gc := flag.Bool("gc", false, "run value-log gc")

	flag.Parse()

	sdb, err := storage.Open(*path, false, *ttl)
	if err != nil {
		log.Fatal(err)
	}

	defer sdb.Close()

	// Echo instance
	e := echo.New()
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
	e.GET("/x/:id/meta", cashier.getMetadata).Name = "Get Metadata"

	go func() {
		// Start server
		if err := e.Start(":1999"); err != nil {
			e.Logger.Warn("Server didn't start")
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
