/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var MigrationsDatabase string
var MigrationsTable string
var MigrationsDirectory string

// migrationsCmd represents the migrations command
var migrationsCmd = &cobra.Command{
	Use:   "migrations",
	Short: "Subcommands for managing ClickHouse migrations",
	Long:  `Subcommands for managing ClickHouse migrations.`,
}

func init() {
	rootCmd.AddCommand(migrationsCmd)

	migrationsCmd.PersistentFlags().StringVar(&MigrationsDatabase, "migrations-database", "migrations", "ClickHouse database to store migrations")
	viper.BindPFlag("migrations-database", migrationsCmd.PersistentFlags().Lookup("migrations-database"))
	viper.BindEnv("migrations-database", "CLICKHOUSE_MIGRATIONS_DATABASE")

	migrationsCmd.PersistentFlags().StringVar(&MigrationsTable, "migrations-table", "migrations", "ClickHouse table to store migrations")
	viper.BindPFlag("migrations-table", migrationsCmd.PersistentFlags().Lookup("migrations-table"))
	viper.BindEnv("migrations-table", "CLICKHOUSE_MIGRATIONS_TABLE")

	migrationsCmd.PersistentFlags().StringVar(&MigrationsDirectory, "migrations-directory", "migrations", "Directory containing migration files")
	viper.BindPFlag("migrations-directory", migrationsCmd.PersistentFlags().Lookup("migrations-directory"))
	viper.BindEnv("migrations-directory", "CLICKHOUSE_MIGRATIONS_DIRECTORY")
}
