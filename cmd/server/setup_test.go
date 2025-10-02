package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type testDriver struct {
	name       string
	driverName string
	uri        string
	cleanup    func()
}

func getTestDrivers(t *testing.T) []testDriver {
	tmpDir := t.TempDir()

	drivers := []testDriver{
		{
			name:       "SQLite",
			driverName: "adbc_driver_sqlite",
			uri:        filepath.Join(tmpDir, "test_sqlite.db"),
			cleanup:    func() {},
		},
		{
			name:       "DuckDB",
			driverName: "duckdb",
			uri:        ":memory:",
			cleanup:    func() {},
		},
	}

	return drivers
}

func setupTestServer(t *testing.T, driver testDriver) (*DummyFlightSQLServer, func()) {
	drv := &drivermgr.Driver{}

	var dbOptions map[string]string
	if driver.driverName == "duckdb" {
		dbOptions = map[string]string{
			"driver":     driver.driverName,
			"entrypoint": "duckdb_adbc_init",
			"path":       driver.uri,
		}
	} else {
		dbOptions = map[string]string{
			"driver":          driver.driverName,
			adbc.OptionKeyURI: driver.uri,
		}
	}

	db, err := drv.NewDatabase(dbOptions)
	if err != nil {
		t.Fatalf("Failed to create test database for %s: %v", driver.name, err)
	}

	server := &DummyFlightSQLServer{
		db:      &db,
		queries: make(map[string]string),
	}
	server.Alloc = memory.DefaultAllocator

	cleanup := func() {
		db.Close()
		driver.cleanup()
		if driver.driverName == "adbc_driver_sqlite" {
			os.Remove(driver.uri)
		}
	}

	return server, cleanup
}

func setupTestData(t *testing.T, server *DummyFlightSQLServer) {
	ctx := context.Background()

	if server.db == nil {
		t.Fatal("Database is nil")
	}

	db := *server.db
	conn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}
	defer stmt.Close()

	// Create a test table
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS test_table (
			id INTEGER PRIMARY KEY,
			name TEXT,
			value REAL
		)
	`

	err = stmt.SetSqlQuery(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to set create table query: %v", err)
	}

	_, err = stmt.ExecuteUpdate(ctx)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	insertSQL := `
		INSERT INTO test_table (id, name, value) VALUES
		(1, 'test1', 1.5),
		(2, 'test2', 2.5),
		(3, 'test3', 3.5)
	`

	err = stmt.SetSqlQuery(insertSQL)
	if err != nil {
		t.Fatalf("Failed to set insert query: %v", err)
	}

	_, err = stmt.ExecuteUpdate(ctx)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}
}

func setupTestSchemas(t *testing.T, server *DummyFlightSQLServer, driver testDriver) {
	ctx := context.Background()

	if server.db == nil {
		t.Fatal("Database is nil")
	}

	db := *server.db
	conn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}
	defer stmt.Close()

	// Setup differs by driver type
	if driver.driverName == "duckdb" {
		// DuckDB supports schemas
		schemaSQL := `CREATE SCHEMA IF NOT EXISTS test_schema`
		err = stmt.SetSqlQuery(schemaSQL)
		if err != nil {
			t.Fatalf("Failed to set create schema query: %v", err)
		}

		_, err = stmt.ExecuteUpdate(ctx)
		if err != nil {
			t.Fatalf("Failed to create schema: %v", err)
		}

		// Create a table in the new schema
		tableSQL := `CREATE TABLE IF NOT EXISTS test_schema.test_table (id INTEGER, name TEXT)`
		err = stmt.SetSqlQuery(tableSQL)
		if err != nil {
			t.Fatalf("Failed to set create table in schema query: %v", err)
		}

		_, err = stmt.ExecuteUpdate(ctx)
		if err != nil {
			t.Fatalf("Failed to create table in schema: %v", err)
		}
	} else {
		// SQLite doesn't have traditional schemas, but we can attach databases
		// Create an additional table that will be in the "main" schema
		tableSQL := `CREATE TABLE IF NOT EXISTS test_schema_table (id INTEGER, name TEXT)`
		err = stmt.SetSqlQuery(tableSQL)
		if err != nil {
			t.Fatalf("Failed to set create test schema table query: %v", err)
		}

		_, err = stmt.ExecuteUpdate(ctx)
		if err != nil {
			t.Fatalf("Failed to create test schema table: %v", err)
		}
	}
}
