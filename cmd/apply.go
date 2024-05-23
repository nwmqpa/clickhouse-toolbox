/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"sort"

	"github.com/nwmqpa/clickhouse-toolbox/pkg/clickhouse_wrapper"
	"github.com/nwmqpa/clickhouse-toolbox/pkg/migrations"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// applyCmd represents the apply command
var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply migrations in the given folder",
	Long:  `Apply migrations in the given folder.`,
	Run: func(cmd *cobra.Command, args []string) {
		migrationFolder := viper.GetString("migrations-directory")

		loadedMigrations, err := migrations.LoadMigrationsDirectory(migrationFolder)

		if err != nil {
			slog.Error("Could not load migrations files", "error", err)
			os.Exit(1)
		}

		sort.Slice(loadedMigrations, func(i, j int) bool {
			return loadedMigrations[i].Datetime.Before(loadedMigrations[j].Datetime)
		})

		clickhouseAddress := viper.GetString("clickhouse-address")
		clickhouseUsername := viper.GetString("clickhouse-username")
		clickhousePassword := viper.GetString("clickhouse-password")

		slog.Info("Connecting to database")

		conn, err := clickhouse_wrapper.ConnectToClickhouse(clickhouseAddress, clickhouseUsername, clickhousePassword)

		if err != nil {
			slog.Error(fmt.Sprintf("Error connecting to Clickhouse: %s", err.Error()))
			os.Exit(1)
		}

		defer conn.Close()

		migrationDatabase := viper.GetString("migrations-database")
		migrationTable := viper.GetString("migrations-table")

		slog.Info("Setting up migration table")

		err = migrations.SetupMigrationTable(conn, migrationDatabase, migrationTable)

		if err != nil {
			slog.Error(fmt.Sprintf("Error setting up migration table: %s", err.Error()))
			os.Exit(1)
		}

		for _, migration := range loadedMigrations {
			if migration.MigrationSide == migrations.MigrationUp {
				isMigrationApplied, err := migration.CheckIfMigrationIsApplied(conn, migrationDatabase, migrationTable)

				if err != nil {
					slog.Error(fmt.Sprintf("Error checking if migration %s is applied: %s", migration.Path, err.Error()))
					os.Exit(1)
				}

				if isMigrationApplied {
					fmt.Printf("Migration %s is already applied\n", migration.Path)
					continue
				}

				fmt.Printf("Applying migration %s\n", migration.Path)
				err = migration.Apply(conn)

				if err != nil {
					slog.Error(fmt.Sprintf("Error applying migration %s: %s", migration.Path, err.Error()))
					os.Exit(1)
				}

				err = migration.StoreMigration(conn, migrationDatabase, migrationTable)

				if err != nil {
					slog.Error(fmt.Sprintf("Error storing migration %s: %s", migration.Path, err.Error()))
					os.Exit(1)
				}
			}
		}
	},
}

func init() {
	migrationsCmd.AddCommand(applyCmd)
}
