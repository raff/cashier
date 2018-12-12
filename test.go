package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	//"strings"
	"time"

	"github.com/raff/cashier/storage"
)

func main() {
	path := flag.String("path", "storage.data", "path to data folder")
	ttl := flag.Duration("ttl", 10*time.Minute, "time to live")
	gc := flag.Bool("gc", false, "run value-log gc")
	scan := flag.Bool("scan", false, "scan current database")
	put := flag.Bool("put", false, "upload new file")
	get := flag.Bool("get", false, "download file")
	stat := flag.Bool("stat", false, "file info")
	ppos := flag.Int64("pos", 0, "file position")
	flag.Parse()

	sdb, err := storage.Open(*path, *ttl)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer sdb.Close()

	if *gc {
		sdb.GC()
	}

	if *scan {
		sdb.Scan("")
	}

	if *stat {
		for _, k := range flag.Args() {
			fmt.Println(sdb.Stat(k))
		}
	}

	if *put {
		var key, fname, fpath string

		switch flag.NArg() {
		case 1:
			fpath = flag.Arg(0)
			fname = filepath.Base(fpath)
			key = fname

		case 2:
			key = flag.Arg(0)
			fpath = flag.Arg(1)
			fname = filepath.Base(fpath)

		default:
			fmt.Println("usage: test -put [key] file")
			return
		}

		f, err := os.Open(fpath)
		if err != nil {
			fmt.Println(err)
			return
		}

		defer f.Close()

		ctype := "application/octet-stream"
		hash := md5.New()

		sz, err := io.Copy(hash, f)
		if err != nil {
			fmt.Println(err)
			return
		}

		if *ppos == 0 {
			// if ppos != 0, assume the file exists, but we didn't finish writing

			if err := sdb.CreateFile(key, fname, ctype, sz, hash.Sum(nil)); err != nil {
				fmt.Println(err)
				return
			}
		}

		var buf = make([]byte, 4*storage.BlockSize)

		for pos := *ppos; pos != storage.FileComplete; {
			n, err := f.ReadAt(buf, pos)
			if err == io.EOF {
				if n != 0 {
					err = nil
				} else {
					fmt.Println("unexpected EOF at", pos, "len", len(buf))
					break
				}
			}
			if err != nil {
				fmt.Println(err)
				return
			}

			npos, err := sdb.WriteAt(key, pos, buf[:n])
			if err != nil {
				fmt.Println(err)
				return
			}

			pos = npos
		}
	}

	if *get {
		var key, fpath string

		switch flag.NArg() {
		case 1:
			key = flag.Arg(0)
			stat, err := sdb.Stat(key)
			if err != nil {
				fmt.Println(err)
				return
			}
			fpath = stat.Name

		case 2:
			key = flag.Arg(0)
			fpath = flag.Arg(1)

		default:
			fmt.Println("usage: test -get key [file]")
			return
		}

		f, err := os.OpenFile(fpath, os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			fmt.Println(err)
			return
		}

		defer f.Close()
	}
}
