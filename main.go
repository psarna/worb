package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/sarna/worb/internal/server"
	"github.com/spf13/cobra"
)

func main() {
	home, _ := os.UserHomeDir()
	defaultDataDir := filepath.Join(home, ".worb")

	var host string
	var port int
	var dataDir string

	cmd := &cobra.Command{
		Use:   "worb",
		Short: "Local, single-binary server compatible with the standard wandb Python client",
		RunE: func(cmd *cobra.Command, args []string) error {
			srv, err := server.New(server.Config{
				Host:    host,
				Port:    port,
				DataDir: dataDir,
			})
			if err != nil {
				return err
			}
			return srv.Run()
		},
	}

	cmd.Flags().StringVar(&host, "host", "127.0.0.1", "host to bind to (use 0.0.0.0 for all interfaces)")
	cmd.Flags().IntVar(&port, "port", 8080, "port to listen on")
	cmd.Flags().StringVar(&dataDir, "data", defaultDataDir, "data directory")

	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
