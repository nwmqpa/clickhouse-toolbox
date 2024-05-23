/*
Copyright © 2024 NAME HERE <thomas.nicollet@synaps.io>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var ClickhouseAddress string
var ClickhouseUsername string
var ClickhousePassword string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "clickhouse-toolbox",
	Short: "Toolbox of utilities for ClickHouse database",
	Long:  `Toolbox of utilities for ClickHouse database.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&ClickhouseAddress, "clickhouse-address", "localhost:9000", "ClickHouse address")
	viper.BindPFlag("clickhouse-address", rootCmd.PersistentFlags().Lookup("clickhouse-address"))
	viper.BindEnv("clickhouse-address", "CLICKHOUSE_ADDRESS")

	rootCmd.PersistentFlags().StringVar(&ClickhouseUsername, "clickhouse-username", "default", "ClickHouse username")
	viper.BindPFlag("clickhouse-username", rootCmd.PersistentFlags().Lookup("clickhouse-username"))
	viper.BindEnv("clickhouse-username", "CLICKHOUSE_USERNAME")

	rootCmd.PersistentFlags().StringVar(&ClickhousePassword, "clickhouse-password", "", "ClickHouse password")
	viper.BindPFlag("clickhouse-password", rootCmd.PersistentFlags().Lookup("clickhouse-password"))
	viper.BindEnv("clickhouse-password", "CLICKHOUSE_PASSWORD")
}
