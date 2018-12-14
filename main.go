package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/raff/cashier/storage"
)

type Cashier struct {
	sdb *storage.StorageDB
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

	return c.JSON(http.StatusOK, info)
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
		log.Println("expected", size, "read", nread)
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

func main() {
	path := flag.String("path", "storage.data", "path to data folder")
	ttl := flag.Duration("ttl", 10*time.Minute, "time to live")
	//gc := flag.Bool("gc", false, "run value-log gc")

	flag.Parse()

	sdb, err := storage.Open(*path, *ttl)
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
	})

	e.GET("/routes", func(c echo.Context) error {
		return c.JSON(http.StatusOK, e.Routes())
	})

	e.GET("/x/:id", cashier.getEntry)
	e.POST("/x/:id", cashier.putEntry)
	e.DELETE("/x/:id", cashier.deleteEntry)

	// Start server
	e.Logger.Fatal(e.Start(":1999"))
}
