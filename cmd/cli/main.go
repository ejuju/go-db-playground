package main

import (
	"fmt"
	"os"

	"github.com/ejuju/go-db-playground/textdb"
)

func main() {
	db, err := textdb.NewDB("test.txt.db")
	if err != nil {
		panic(err)
	}

	switch os.Args[1] {
	case "set":
		err = db.Set(os.Args[2])
	case "exists":
		ok := db.Exists(os.Args[2])
		fmt.Printf("-> exists %q: %v\n", os.Args[2], ok)
	case "delete":
		err = db.Delete(os.Args[2])
	case "put":
		err = db.Put(os.Args[2], []byte(os.Args[3]))
	case "get":
		var v []byte
		v, err = db.Get(os.Args[2])
		fmt.Printf("-> %q\n", v)
	case "find":
		var v []byte
		v, err = db.Find(os.Args[2])
		fmt.Printf("-> %q\n", v)
	}
	if err != nil {
		panic(err)
	}
}
