/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/spf13/cobra"
	"github.com/vasilii314/orchestrator/manager"
	"github.com/vasilii314/orchestrator/scheduler"
	"github.com/vasilii314/orchestrator/store"
	"log"
)

// managerCmd represents the manager command
var managerCmd = &cobra.Command{
	Use:   "manager",
	Short: "Manager command to operate an orchestrator manager node.",
	Long: `Orchestrator manager command.

The manager controls the orchestration system and is responsible for:
- Accepting tasks from users
- Scheduling tasks onto worker nodes
- Rescheduling tasks in the event of a node failure
- Periodically polling workers to get task updates`,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetInt("port")
		workers, _ := cmd.Flags().GetStringSlice("workers")
		schedulerType, _ := cmd.Flags().GetString("scheduler")
		storeType, _ := cmd.Flags().GetString("store")
		log.Println("Starting manager")
		m := manager.New(workers, scheduler.SchedulerType(schedulerType), store.StoreType(storeType))
		api := manager.Api{Address: host, Port: port, Manager: m}
		go m.ProcessTasks()
		go m.UpdateTasks()
		go m.DoHealthChecks()
		log.Printf("Starting manager API on http://%s:%d", host, port)
		api.Start()
	},
}

func init() {
	rootCmd.AddCommand(managerCmd)
	managerCmd.Flags().StringP("host", "H", "localhost", "Hostname or IP address")
	managerCmd.Flags().IntP("port", "p", 5554, "Port on which to listen")
	managerCmd.Flags().StringSliceP("workers", "w", []string{"localhost:5555"}, "List of workers on which the manager will schedule the tasks")
	managerCmd.Flags().StringP("scheduler", "s", "epvm", "Type of scheduler to use")
	managerCmd.Flags().StringP("store", "s", "memory", "Type of datastore to use for tasks (\"memory\" or \"persistent\")")
}
