package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/sarna/worb/internal/server"
)

func main() {
	home, _ := os.UserHomeDir()
	defaultDataDir := filepath.Join(home, ".worb")

	port := flag.Int("port", 8080, "port to listen on")
	dataDir := flag.String("data", defaultDataDir, "data directory")
	flag.Parse()

	srv, err := server.New(server.Config{
		Port:    *port,
		DataDir: *dataDir,
	})
	if err != nil {
		log.Fatalf("failed to start: %v", err)
	}

	if err := srv.Run(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
