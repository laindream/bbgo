package main

import (
	"github.com/c9s/bbgo/pkg/cmd"
	"log"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6063", nil))
	}()
	cmd.Execute()
}
