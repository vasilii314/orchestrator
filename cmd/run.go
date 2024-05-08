/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !errors.Is(err, fs.ErrNotExist)
}

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a new task",
	Long: `Orchestrator run command.
The run command starts a new task.`,
	Run: func(cmd *cobra.Command, args []string) {
		manager, _ := cmd.Flags().GetString("manager")
		filename, _ := cmd.Flags().GetString("filename")
		fullFilePath, err := filepath.Abs(filename)
		if err != nil {
			log.Fatal(err)
		}
		if !fileExists(filename) {
			log.Fatalf("File %s does not exist\n", filename)
		}
		log.Printf("Using manager: %v\n", manager)
		log.Printf("Using file: %v\n", fullFilePath)
		data, err := os.ReadFile(filename)
		if err != nil {
			log.Fatalf("Cannot read file: %v\n", filename)
		}
		log.Printf("Data: %v\n", string(data))
		url := fmt.Sprintf("http://%s/tasks", manager)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Fatal(err)
		}
		if resp.StatusCode != http.StatusCreated {
			log.Printf("Error sending request: %v\n", resp.StatusCode)
		}
		defer resp.Body.Close()
		log.Println("Successfully sent task request to manager")
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().StringP("manager", "m", "localhost:5554", "Manager to submit tasks to")
	runCmd.Flags().StringP("filename", "f", "task.json", "Task specification file")
}
