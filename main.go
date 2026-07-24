package main

import (
	"bufio"
	"context"
	"errors"
	"exchange-ws/config"
	"exchange-ws/eureka"
	"exchange-ws/process"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
)

const logTimeLayout = "2006/01/02 15:04:05.000000"

func timestampStdout() func() {
	out := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		log.Printf("stdout timestamps disabled: %v", err)
		return func() {}
	}
	os.Stdout = w

	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReader(r)
		for {
			line, err := br.ReadBytes('\n')
			if len(line) > 0 {
				fmt.Fprintf(out, "%s %s", time.Now().UTC().Format(logTimeLayout), line)
			}
			if err != nil {
				return
			}
		}
	}()

	return func() {
		os.Stdout = out
		_ = w.Close()
		<-done
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	flushStdout := timestampStdout()
	defer flushStdout()

	e := config.LoadConfig()
	if e != nil {
		fmt.Println(e)
	}
	go eureka.CreateEurekaClient("config")

	ctx, cancel := context.WithCancel(context.Background())
	manager := process.NewManager(ctx)

	r := chi.NewRouter()

	r.Get("/config/{config}", config.GetConfigHandler)
	r.Put("/config/{config}", config.SetConfigHandler)

	r.Post("/start/{config}",
		func(w http.ResponseWriter, r *http.Request) {
			process.BbtHandler(w, r, manager)
		})

	r.Post("/stop/{config}",
		func(w http.ResponseWriter, r *http.Request) {
			process.BbtHandlerStop(w, r, manager)
		})

	r.Post("/info/{config}", process.InfoHandler)

	r.Post("/kline/{config}", process.KlineHandler)

	go func() {
		if err := http.ListenAndServe(":8080", r); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop
	fmt.Println("Shutting down…")

	shutdownDone := make(chan struct{})
	go func() {
		manager.Shutdown()
		cancel()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
	case <-time.After(30 * time.Second):
		fmt.Println("Shutdown timeout")
	}

}
