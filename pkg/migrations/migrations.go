package migrations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/oriser/regroup"
)

type MigrationSide int

const (
	MigrationUp MigrationSide = iota
	MigrationDown
)

type Migration struct {
	Name          string
	Path          string
	Identifier    string
	Datetime      time.Time
	MigrationSide MigrationSide
}

var MigrationFilenameRegex = regroup.MustCompile(`^(?P<datetime>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})_(?P<name>[^.]*)\.(?P<side>down|up)\.sql$`)

type MigrationName struct {
	Datetime string `regroup:"datetime"`
	Name     string `regroup:"name"`
	Side     string `regroup:"side"`
}

// Parse filenames of the kind:
// YYYY-MM-DD_HH-MM-SS_migration_name.{up|down}.sql
func parseMigrationFilename(migrationDirectory, filename, identifier string) (*Migration, error) {
	var migrationName MigrationName

	if err := MigrationFilenameRegex.MatchToTarget(filename, &migrationName); err != nil {
		return nil, err
	}

	datetime, err := time.Parse("2006-01-02_15-04-05", migrationName.Datetime)

	if err != nil {
		return nil, err
	}

	path := fmt.Sprintf("%s%c%s", migrationDirectory, os.PathSeparator, filename)

	if migrationName.Side == "up" {
		return &Migration{
			Name:          migrationName.Name,
			Path:          path,
			Datetime:      datetime,
			Identifier:    identifier,
			MigrationSide: MigrationUp,
		}, nil
	} else {
		return &Migration{
			Name:          migrationName.Name,
			Path:          path,
			Datetime:      datetime,
			Identifier:    identifier,
			MigrationSide: MigrationDown,
		}, nil
	}
}

func LoadMigrationsDirectory(migrationDirectory, migrationIdentifier string) ([]Migration, error) {
	dirEntries, err := os.ReadDir(migrationDirectory)

	if err != nil {
		return nil, err
	}

	migrations := make([]Migration, 0)

	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() {
			continue
		}

		migration, err := parseMigrationFilename(migrationDirectory, dirEntry.Name(), migrationIdentifier)

		if err != nil {
			return nil, err
		}

		migrations = append(migrations, *migration)
	}

	return migrations, nil
}

func SetupMigrationTable(conn driver.Conn, migrationDatabase, migrationTable, migrationTableStoragePolicy string) error {
	err := conn.Exec(context.Background(), fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s;`, migrationDatabase))

	if err != nil {
		return err
	}

	return conn.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			datetime DateTime,
			name String,
			identifier String,
			checksum String,
		)
		ENGINE = MergeTree
		ORDER BY (datetime, name)
		SETTINGS storage_policy = '%s';
	`,
		migrationDatabase,
		migrationTable,
		migrationTableStoragePolicy,
	))
}

func (m *Migration) CheckIfMigrationIsApplied(conn driver.Conn, migrationDatabase, migrationTable string) (bool, error) {
	rows, err := conn.Query(
		context.Background(),
		fmt.Sprintf("SELECT datetime, name, checksum FROM %s.%s WHERE name = ? AND datetime = ? AND identifier = ?", migrationDatabase, migrationTable),
		m.Name,
		m.Datetime,
		m.Identifier,
	)

	if err != nil {
		return false, err
	}

	defer rows.Close()

	for rows.Next() {
		var rowDatetime time.Time
		var rowName string
		var rowChecksum string

		if err := rows.Scan(&rowDatetime, &rowName, &rowChecksum); err != nil {
			return false, err
		}

		currentChecksum, err := m.ComputeChecksum()

		if err != nil {
			return false, err
		}

		if currentChecksum != rowChecksum {
			return false, fmt.Errorf("checksum mismatch: expected %s, got %s", rowChecksum, currentChecksum)
		} else {
			return true, nil
		}
	}

	return false, nil
}

func (m *Migration) StoreMigration(conn driver.Conn, migrationDatabase, migrationTable string) error {
	checksum, err := m.ComputeChecksum()

	if err != nil {
		return err
	}

	return conn.Exec(
		context.Background(),
		fmt.Sprintf("INSERT INTO %s.%s (datetime, name, identifier, checksum) VALUES (?, ?, ?, ?)", migrationDatabase, migrationTable),
		m.Datetime,
		m.Name,
		m.Identifier,
		checksum,
	)
}

func (m *Migration) RemoveMigration(conn driver.Conn, migrationDatabase, migrationTable string) error {
	return conn.Exec(
		context.Background(),
		fmt.Sprintf("DELETE FROM %s.%s WHERE name = ? AND datetime = ? AND identifier = ?", migrationDatabase, migrationTable),
		m.Name,
		m.Datetime,
		m.Identifier,
	)
}

func (m *Migration) ComputeChecksum() (string, error) {
	file, err := os.Open(m.Path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()

	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (m *Migration) Apply(conn driver.Conn) error {
	migrationData, err := os.ReadFile(m.Path)

	if err != nil {
		return err
	}

	migrationContent := string(migrationData)

	migrationContentLines := strings.Split(migrationContent, ";")
	migrationStatements := make([]string, 0)

	for _, statement := range migrationContentLines {
		if strings.TrimSpace(statement) == "" {
			continue
		}

		migrationStatements = append(migrationStatements, strings.TrimSpace(statement))
	}

	for _, statement := range migrationStatements {
		err := conn.Exec(context.Background(), statement)

		if err != nil {
			return fmt.Errorf("cannot execute statement `%s`: %s", statement, err)
		}
	}

	return nil
}

func (m *Migration) FindMatchingMigration(migrations []Migration) *Migration {
	for _, migration := range migrations {
		if migration.Name == m.Name && migration.MigrationSide != m.MigrationSide {
			return &migration
		}
	}

	return nil
}
