package main

import (
	"net/http"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

type Cashier struct {
	cache map[string]interface{}
}

func (cc *Cashier) getEntry(c echo.Context) error {
	id := c.Param("id")
	if v, ok := cc.cache[id]; ok {
		return c.JSON(http.StatusOK, map[string]interface{}{id: v})
	}

	return c.String(http.StatusNotFound, "NOT FOUND")
}

func (cc *Cashier) putEntry(c echo.Context) error {
	id := c.Param("id")
	if _, ok := cc.cache[id]; ok {
		return c.JSON(http.StatusConflict, "ALREADY EXISTS")
	}

	cc.cache[id] = id
	return c.String(http.StatusCreated, "CREATED")
}

func (cc *Cashier) deleteEntry(c echo.Context) error {
	id := c.Param("id")
	delete(cc.cache, id)
	return c.String(http.StatusNoContent, "DELETED")
}

var (
	Cache = &Cashier{cache: map[string]interface{}{}}
)

func main() {
	// Echo instance
	e := echo.New()

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

	e.GET("/x/:id", Cache.getEntry)
	e.POST("/x/:id", Cache.putEntry)
	e.DELETE("/x/:id", Cache.deleteEntry)

	// Start server
	e.Logger.Fatal(e.Start(":1999"))
}
