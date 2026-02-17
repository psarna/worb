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

	var port int
	var dataDir string

	cmd := &cobra.Command{
		Use:   "worb",
		Short: "Local, single-binary server compatible with the standard wandb Python client",
		RunE: func(cmd *cobra.Command, args []string) error {
			srv, err := server.New(server.Config{
				Port:    port,
				DataDir: dataDir,
			})
			if err != nil {
				return err
			}
			return srv.Run()
		},
	}

	cmd.Flags().IntVar(&port, "port", 8080, "port to listen on")
	cmd.Flags().StringVar(&dataDir, "data", defaultDataDir, "data directory")

	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
