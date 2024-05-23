/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nwmqpa/clickhouse-toolbox/pkg/clickhouse_wrapper"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var DictionaryName string
var DictionaryDatabase string

type DictionarySynchronizationData struct {
	Keys   []string                 `json:"keys"`
	Values []map[string]interface{} `json:"values"`
}

func insertDataInTable(conn driver.Conn, sourceTable string, sourceData DictionarySynchronizationData) error {
	keys := sourceData.Keys

	rows := make([][]interface{}, 0)

	for _, values := range sourceData.Values {
		data := make([]interface{}, 0)

		for _, key := range keys {
			if _, ok := values[key]; !ok {
				return fmt.Errorf("missing key '%s' in values", key)
			}
			data = append(data, values[key])
		}

		rows = append(rows, data)
	}

	return clickhouse_wrapper.BatchInsertDataInTable(conn, sourceTable, rows)
}

// syncDictionaryCmd represents the syncDictionary command
var syncDictionaryCmd = &cobra.Command{
	Use:   "syncDictionary",
	Short: "Synchronize dictionary from the given sink into Clickhouse",
	Long:  `Synchronize dictionary from the given sink into Clickhouse.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			slog.Error("No source file provided")
			os.Exit(1)
		}

		var sourceData DictionarySynchronizationData

		if args[0] == "-" {
			slog.Info("Reading dictionnary data from stdin")

			bytes, err := io.ReadAll(os.Stdin)

			if err != nil {
				slog.Error(fmt.Sprintf("Error reading dictionnary data from stdin: %s", err.Error()))
				os.Exit(1)
			}

			if err := json.Unmarshal(bytes, &sourceData); err != nil {
				slog.Error(fmt.Sprintf("Error unmarshalling dictionnary data from stdin: %s", err.Error()))
				os.Exit(1)
			}
		} else {
			slog.Info(fmt.Sprintf("Reading dictionnary data from file: %s", args[0]))

			bytes, err := os.ReadFile(args[0])

			if err != nil {
				slog.Error(fmt.Sprintf("Error reading dictionnary data from file: %s", err.Error()))
				os.Exit(1)
			}

			if err := json.Unmarshal(bytes, &sourceData); err != nil {
				slog.Error(fmt.Sprintf("Error unmarshalling dictionnary data from stdin: %s", err.Error()))
				os.Exit(1)
			}
		}

		slog.Info("Synchronizing dictionary")

		clickhouseAddress := viper.GetString("clickhouse-address")
		clickhouseUsername := viper.GetString("clickhouse-username")
		clickhousePassword := viper.GetString("clickhouse-password")

		conn, err := clickhouse_wrapper.ConnectToClickhouse(clickhouseAddress, clickhouseUsername, clickhousePassword)

		if err != nil {
			slog.Error(fmt.Sprintf("Error connecting to Clickhouse: %s", err.Error()))
			os.Exit(1)
		}

		defer conn.Close()

		dictionaryDatabase := viper.GetString("dictionary-database")
		dictionaryName := viper.GetString("dictionary-name")

		slog.Info(fmt.Sprintf("Initial reload of dictionary: %s.%s", dictionaryDatabase, dictionaryName))

		err = clickhouse_wrapper.ReloadDictionary(conn, dictionaryDatabase, dictionaryName)

		if err != nil {
			slog.Error(fmt.Sprintf("Error reloading dictionary: %s", err.Error()))
			os.Exit(1)
		}

		sourceTable, err := clickhouse_wrapper.GetDictionarySourceTable(conn, dictionaryDatabase, dictionaryName)

		if err != nil {
			slog.Error(fmt.Sprintf("Error getting dictionary source table: %s", err.Error()))
			os.Exit(1)
		}

		slog.Info(fmt.Sprintf("Dictionary source table is: %s", sourceTable))

		slog.Info(fmt.Sprintf("Cleaning up source table: %s", sourceTable))

		err = clickhouse_wrapper.CleanupTable(conn, sourceTable)

		if err != nil {
			slog.Error(fmt.Sprintf("Error cleaning up source table: %s", err.Error()))
			os.Exit(1)
		}

		slog.Info(fmt.Sprintf("Inserting data in source table: %s", sourceTable))

		err = insertDataInTable(conn, sourceTable, sourceData)

		if err != nil {
			slog.Error(fmt.Sprintf("Error inserting data in source table: %s", err.Error()))
			os.Exit(1)
		}

		slog.Info(fmt.Sprintf("Reloading dictionary: %s", dictionaryName))

		err = clickhouse_wrapper.ReloadDictionary(conn, dictionaryDatabase, dictionaryName)

		if err != nil {
			slog.Error(fmt.Sprintf("Error reloading dictionary: %s", err.Error()))
			os.Exit(1)
		}

		slog.Info("Dictionary synchronized")
	},
}

func init() {
	rootCmd.AddCommand(syncDictionaryCmd)

	syncDictionaryCmd.PersistentFlags().StringVar(&DictionaryName, "dictionary-database", "", "ClickHouse Dictionary database to synchronize")
	viper.BindPFlag("dictionary-database", syncDictionaryCmd.PersistentFlags().Lookup("dictionary-database"))
	viper.BindEnv("dictionary-database", "CLICKHOUSE_DICTIONARY_DATABASE")

	syncDictionaryCmd.PersistentFlags().StringVar(&DictionaryDatabase, "dictionary-name", "", "ClickHouse Dictionary name to synchronize")
	viper.BindPFlag("dictionary-name", syncDictionaryCmd.PersistentFlags().Lookup("dictionary-name"))
	viper.BindEnv("dictionary-name", "CLICKHOUSE_DICTIONARY_NAME")
}
