package clickhouse_wrapper

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func GetDictionarySourceTable(conn driver.Conn, dictionaryDatabase, dictionaryName string) (string, error) {
	var (
		ctx = context.Background()
	)

	row := conn.QueryRow(ctx, "SELECT source FROM system.dictionaries WHERE database = ? AND name = ?", dictionaryDatabase, dictionaryName)

	var source string

	if err := row.Scan(&source); err != nil {
		return "", err
	}

	if !strings.Contains(source, "ClickHouse: ") {
		return "", fmt.Errorf("dictionary source is not a ClickHouse table: Source is '%s'", source)
	}

	return strings.Replace(source, "ClickHouse: ", "", 1), nil
}

func CleanupTable(conn driver.Conn, sourceTable string) error {
	var (
		ctx = context.Background()
	)

	return conn.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE 1 == 1", sourceTable))
}

func ReloadDictionary(conn driver.Conn, dictionaryDatabase, dictionaryName string) error {
	var (
		ctx = context.Background()
	)

	return conn.Exec(ctx, "SYSTEM RELOAD DICTIONARY ?", fmt.Sprintf("%s.%s", dictionaryDatabase, dictionaryName))
}

func ConnectToClickhouse(clickhouseAddress, clickhoustUsername, clickhousePassword string) (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{clickhouseAddress},
			Auth: clickhouse.Auth{
				Username: clickhoustUsername,
				Password: clickhousePassword,
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "clickhouse-toolbox", Version: "0.1.0"},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				slog.Debug(fmt.Sprintf(format, v))
			},
			TLS: nil,
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			slog.Error(fmt.Sprintf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace))
		}
		return nil, err
	}

	rows, err := conn.Query(context.Background(), "SELECT 1")

	if err != nil {
		slog.Error(fmt.Sprintf("Error querying Clickhouse: %s", err.Error()))
		conn.Close()
		return nil, err
	}

	for rows.Next() {
		var dummy uint8
		if err := rows.Scan(&dummy); err != nil {
			slog.Error(fmt.Sprintf("Error scanning Clickhouse: %s", err.Error()))
			conn.Close()
			return nil, err
		}
	}

	return conn, nil
}

func BatchInsertDataInTable(conn driver.Conn, table string, rows [][]interface{}) error {
	var (
		ctx = context.Background()
	)

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", table))

	if err != nil {
		return err
	}

	for _, row := range rows {
		err = batch.Append(row...)

		if err != nil {
			return err
		}
	}

	return batch.Send()
}
