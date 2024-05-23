/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new migration",
	Long:  `Create a new migration.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			slog.Error("No migration name provided")
			os.Exit(1)
		}

		migrationFolder := viper.GetString("migrations-directory")

		slog.Info("Creating migration", "name", args[0])

		creationDate := time.Now().Format(time.DateOnly)
		creationTime := time.Now().Format(time.TimeOnly)

		creationDatetime := fmt.Sprintf("%s_%s", creationDate, strings.ReplaceAll(creationTime, ":", "-"))

		upMigrationFile := fmt.Sprintf("%s/%s_%s.up.sql", migrationFolder, creationDatetime, args[0])

		_, err := os.Create(upMigrationFile)

		if err != nil {
			slog.Error("Failed to create up migration file", "error", err)
			os.Exit(1)
		}

		slog.Info("Created up migration file", "file", upMigrationFile)

		downMigrationFile := fmt.Sprintf("%s/%s_%s.down.sql", migrationFolder, creationDatetime, args[0])

		_, err = os.Create(downMigrationFile)

		if err != nil {
			slog.Error("Failed to create down migration file", "error", err)
			os.Exit(1)
		}

		slog.Info("Created down migration file", "file", downMigrationFile)
	},
}

func init() {
	migrationsCmd.AddCommand(createCmd)
}
