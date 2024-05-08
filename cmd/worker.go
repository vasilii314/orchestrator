/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/vasilii314/orchestrator/store"
	"github.com/vasilii314/orchestrator/worker"
	"log"
)

// workerCmd represents the worker command
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Worker command to operate an orchestrator worker node.",
	Long: `Orchestrator worker command.

The worker runs tasks and responds to the manager's requests about task state.
`,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetInt("port")
		name, _ := cmd.Flags().GetString("name")
		s, _ := cmd.Flags().GetString("store")
		log.Printf("Starting worker %s", name)
		w := worker.New(name, store.StoreType(s))
		api := worker.Api{Address: host, Port: port, Worker: w}
		go w.RunTasks()
		go w.CollectStats()
		go w.UpdateTasks()
		log.Printf("Starting worker API on http://%s:%d", host, port)
		api.Start()
	},
}

func init() {
	rootCmd.AddCommand(workerCmd)
	workerCmd.Flags().StringP("host", "H", "localhost", "Hostname or IP address")
	workerCmd.Flags().IntP("port", "p", 5555, "Port on which to listen")
	workerCmd.Flags().StringP("name", "n", fmt.Sprintf("worker-%s", uuid.New().String()), "Name of the worker")
	workerCmd.Flags().StringP("store", "s", "memory", "Type of datastore to use for tasks (\"memory\" or \"persistent\")")
}
