package main

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestDoGetCatalogs(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			ctx := context.Background()

			// Call DoGetCatalogs
			schema, streamCh, err := server.DoGetCatalogs(ctx)
			if err != nil {
				t.Fatalf("DoGetCatalogs failed for %s: %v", driver.name, err)
			}

			// Verify schema matches expected catalog schema
			expectedSchema := schema_ref.Catalogs
			if !schema.Equal(expectedSchema) {
				t.Errorf("Schema mismatch for %s.\nExpected: %v\nGot: %v", driver.name, expectedSchema, schema)
			}

			// Collect all records from the stream
			var records []arrow.RecordBatch
			for chunk := range streamCh {
				if chunk.Data != nil {
					records = append(records, chunk.Data)
				}
			}

			// Verify we got at least one record (default catalog should exist)
			if len(records) == 0 {
				t.Errorf("Expected at least one catalog record for %s, got none", driver.name)
				return
			}

			// Verify first record structure
			record := records[0]
			if record.NumCols() != 1 {
				t.Errorf("Expected 1 column in catalog record for %s, got %d", driver.name, record.NumCols())
			}

			// Verify the column is a string array (catalog_name)
			catalogCol, ok := record.Column(0).(*array.String)
			if !ok {
				t.Errorf("Expected string array for catalog column in %s, got %T", driver.name, record.Column(0))
				return
			}

			// Verify we have at least one catalog name
			if catalogCol.Len() == 0 {
				t.Errorf("Expected at least one catalog name for %s, got empty array", driver.name)
				return
			}

			// Log the catalog names for debugging
			t.Logf("Found %d catalogs for %s:", catalogCol.Len(), driver.name)
			for i := 0; i < catalogCol.Len(); i++ {
				if catalogCol.IsValid(i) {
					t.Logf("  - %s", catalogCol.Value(i))
				}
			}
		})
	}
}

func TestDoGetDBSchemas(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			// Setup test schemas
			setupTestSchemas(t, server, driver)

			ctx := context.Background()

			// Create GetDBSchemas command (no filters)
			cmd := &mockGetDBSchemas{
				catalog:    nil,
				dbSchemaFilterPattern: nil,
			}

			// Call DoGetDBSchemas
			schema, streamCh, err := server.DoGetDBSchemas(ctx, cmd)
			if err != nil {
				t.Fatalf("DoGetDBSchemas failed for %s: %v", driver.name, err)
			}

			// Verify schema matches expected schema
			expectedSchema := schema_ref.DBSchemas
			if !schema.Equal(expectedSchema) {
				t.Errorf("Schema mismatch for %s.\nExpected: %v\nGot: %v", driver.name, expectedSchema, schema)
			}

			// Collect all records from the stream
			var records []arrow.RecordBatch
			var allSchemas []string
			var allCatalogs []string

			for chunk := range streamCh {
				if chunk.Data != nil {
					records = append(records, chunk.Data)
					record := chunk.Data

					// Verify record structure
					if record.NumCols() != 2 {
						t.Errorf("Expected 2 columns in schema record for %s, got %d", driver.name, record.NumCols())
						continue
					}

					// Extract catalog and schema names
					catalogCol, ok := record.Column(0).(*array.String)
					if !ok {
						t.Errorf("Expected string array for catalog column in %s, got %T", driver.name, record.Column(0))
						continue
					}

					schemaCol, ok := record.Column(1).(*array.String)
					if !ok {
						t.Errorf("Expected string array for schema column in %s, got %T", driver.name, record.Column(1))
						continue
					}

					// Collect all schema and catalog names
					for i := 0; i < catalogCol.Len(); i++ {
						if catalogCol.IsValid(i) && schemaCol.IsValid(i) {
							allCatalogs = append(allCatalogs, catalogCol.Value(i))
							allSchemas = append(allSchemas, schemaCol.Value(i))
						}
					}
				}
			}

			// Verify we got at least one schema
			if len(allSchemas) == 0 {
				t.Errorf("Expected at least one schema for %s, got none", driver.name)
				return
			}

			// Log the schemas for debugging
			t.Logf("Found %d schemas for %s:", len(allSchemas), driver.name)
			for i, schema := range allSchemas {
				t.Logf("  - Catalog: %s, Schema: %s", allCatalogs[i], schema)
			}

			// Verify we have the expected schemas based on driver type
			if driver.driverName == "duckdb" {
				// DuckDB should have main, information_schema, and our test_schema
				hasMain := false
				hasTestSchema := false
				for _, schema := range allSchemas {
					if schema == "main" {
						hasMain = true
					}
					if schema == "test_schema" {
						hasTestSchema = true
					}
				}
				if !hasMain {
					t.Errorf("Expected 'main' schema in DuckDB results, but not found")
				}
				if !hasTestSchema {
					t.Errorf("Expected 'test_schema' in DuckDB results, but not found")
				}
			} else {
				// SQLite should have at least one schema (might be empty string for main)
				// SQLite's ADBC driver may return empty schema names for the default schema
				hasMainOrEmpty := false
				for _, schema := range allSchemas {
					if schema == "main" || schema == "" {
						hasMainOrEmpty = true
						break
					}
				}
				if !hasMainOrEmpty {
					t.Errorf("Expected 'main' or empty schema in SQLite results, but not found. Got: %v", allSchemas)
				}
			}
		})
	}
}

func TestDoGetDBSchemasWithFilter(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			// Setup test schemas
			setupTestSchemas(t, server, driver)

			ctx := context.Background()

			// Test with schema filter (only for DuckDB since it supports custom schemas)
			if driver.driverName == "duckdb" {
				filterPattern := "test_%"
				cmd := &mockGetDBSchemas{
					catalog:    nil,
					dbSchemaFilterPattern: &filterPattern,
				}

				_, streamCh, err := server.DoGetDBSchemas(ctx, cmd)
				if err != nil {
					t.Fatalf("DoGetDBSchemas with filter failed for %s: %v", driver.name, err)
				}

				var filteredSchemas []string
				for chunk := range streamCh {
					if chunk.Data != nil {
						record := chunk.Data
						schemaCol := record.Column(1).(*array.String)
						for i := 0; i < schemaCol.Len(); i++ {
							if schemaCol.IsValid(i) {
								filteredSchemas = append(filteredSchemas, schemaCol.Value(i))
							}
						}
					}
				}

				// Verify all returned schemas match the filter pattern
				for _, schema := range filteredSchemas {
					if schema != "test_schema" {
						t.Errorf("Schema filter test failed for %s: expected only 'test_schema', but got '%s'", driver.name, schema)
					}
				}

				t.Logf("Filtered schemas for %s: %v", driver.name, filteredSchemas)
			}
		})
	}
}

// Mock implementation of GetDBSchemas command
type mockGetDBSchemas struct {
	catalog               *string
	dbSchemaFilterPattern *string
}

func (m *mockGetDBSchemas) GetCatalog() *string {
	return m.catalog
}

func (m *mockGetDBSchemas) GetDBSchemaFilterPattern() *string {
	return m.dbSchemaFilterPattern
}

func TestDoGetTables(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			// Setup test data
			setupTestData(t, server)
			setupTestSchemas(t, server, driver)

			ctx := context.Background()

			// Create GetTables command (no filters)
			cmd := &mockGetTables{
				catalog:               nil,
				dbSchemaFilterPattern: nil,
				tableNameFilterPattern: nil,
				tableTypes:            nil,
				includeSchema:         false,
			}

			// Call DoGetTables
			schema, streamCh, err := server.DoGetTables(ctx, cmd)
			if err != nil {
				t.Fatalf("DoGetTables failed for %s: %v", driver.name, err)
			}

			// Verify schema matches expected schema
			expectedSchema := schema_ref.Tables
			if !schema.Equal(expectedSchema) {
				t.Errorf("Schema mismatch for %s.\nExpected: %v\nGot: %v", driver.name, expectedSchema, schema)
			}

			// Collect all records from the stream
			var records []arrow.RecordBatch
			var allTables []string
			var allTableTypes []string
			var allCatalogs []string
			var allSchemas []string

			for chunk := range streamCh {
				if chunk.Data != nil {
					records = append(records, chunk.Data)
					record := chunk.Data

					// Verify record structure
					if record.NumCols() != 4 {
						t.Errorf("Expected 4 columns in table record for %s, got %d", driver.name, record.NumCols())
						continue
					}

					// Extract table metadata
					catalogCol, ok := record.Column(0).(*array.String)
					if !ok {
						t.Errorf("Expected string array for catalog column in %s, got %T", driver.name, record.Column(0))
						continue
					}

					schemaCol, ok := record.Column(1).(*array.String)
					if !ok {
						t.Errorf("Expected string array for schema column in %s, got %T", driver.name, record.Column(1))
						continue
					}

					tableCol, ok := record.Column(2).(*array.String)
					if !ok {
						t.Errorf("Expected string array for table column in %s, got %T", driver.name, record.Column(2))
						continue
					}

					typeCol, ok := record.Column(3).(*array.String)
					if !ok {
						t.Errorf("Expected string array for type column in %s, got %T", driver.name, record.Column(3))
						continue
					}

					// Collect all table metadata
					for i := 0; i < catalogCol.Len(); i++ {
						if catalogCol.IsValid(i) && schemaCol.IsValid(i) && tableCol.IsValid(i) && typeCol.IsValid(i) {
							allCatalogs = append(allCatalogs, catalogCol.Value(i))
							allSchemas = append(allSchemas, schemaCol.Value(i))
							allTables = append(allTables, tableCol.Value(i))
							allTableTypes = append(allTableTypes, typeCol.Value(i))
						}
					}
				}
			}

			// Verify we got at least one table
			if len(allTables) == 0 {
				t.Errorf("Expected at least one table for %s, got none", driver.name)
				return
			}

			// Log the tables for debugging
			t.Logf("Found %d tables for %s:", len(allTables), driver.name)
			for i, table := range allTables {
				t.Logf("  - Catalog: %s, Schema: %s, Table: %s, Type: %s", allCatalogs[i], allSchemas[i], table, allTableTypes[i])
			}

			// Verify we have our test table
			hasTestTable := false
			for _, table := range allTables {
				if table == "test_table" {
					hasTestTable = true
					break
				}
			}
			if !hasTestTable {
				t.Errorf("Expected 'test_table' in results for %s, but not found. Got tables: %v", driver.name, allTables)
			}

			// Verify table types are reasonable
			for _, tableType := range allTableTypes {
				if tableType != "TABLE" && tableType != "VIEW" && tableType != "SYSTEM TABLE" && tableType != "BASE TABLE" && tableType != "table" {
					t.Logf("Found unusual table type for %s: %s", driver.name, tableType)
				}
			}
		})
	}
}

func TestDoGetTablesWithFilters(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			// Setup test data
			setupTestData(t, server)
			setupTestSchemas(t, server, driver)

			ctx := context.Background()

			// Test with table name filter
			tableFilter := "test_%"
			cmd := &mockGetTables{
				catalog:               nil,
				dbSchemaFilterPattern: nil,
				tableNameFilterPattern: &tableFilter,
				tableTypes:            nil,
				includeSchema:         false,
			}

			_, streamCh, err := server.DoGetTables(ctx, cmd)
			if err != nil {
				t.Fatalf("DoGetTables with table filter failed for %s: %v", driver.name, err)
			}

			var filteredTables []string
			for chunk := range streamCh {
				if chunk.Data != nil {
					record := chunk.Data
					tableCol := record.Column(2).(*array.String)
					for i := 0; i < tableCol.Len(); i++ {
						if tableCol.IsValid(i) {
							filteredTables = append(filteredTables, tableCol.Value(i))
						}
					}
				}
			}

			// Verify all returned tables match the filter pattern
			for _, table := range filteredTables {
				if len(table) < 5 || table[:5] != "test_" {
					t.Errorf("Table filter test failed for %s: expected tables starting with 'test_', but got '%s'", driver.name, table)
				}
			}

			t.Logf("Filtered tables for %s: %v", driver.name, filteredTables)
		})
	}
}

func TestDoGetTablesWithTypeFilter(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			// Setup test data
			setupTestData(t, server)

			ctx := context.Background()

			// Test with table type filter (only tables, not views)
			// Note: Different drivers may have different capabilities for type filtering
			var tableTypes []string
			if driver.driverName == "duckdb" {
				// DuckDB may not support table type filtering yet
				tableTypes = nil
			} else {
				// SQLite uses lowercase "table"
				tableTypes = []string{"table", "TABLE", "BASE TABLE"}
			}

			cmd := &mockGetTables{
				catalog:               nil,
				dbSchemaFilterPattern: nil,
				tableNameFilterPattern: nil,
				tableTypes:            tableTypes,
				includeSchema:         false,
			}

			_, streamCh, err := server.DoGetTables(ctx, cmd)
			if err != nil {
				// Skip this test if table type filtering is not supported
				if driver.driverName == "duckdb" {
					t.Skipf("Table type filtering not supported for %s: %v", driver.name, err)
					return
				}
				t.Fatalf("DoGetTables with type filter failed for %s: %v", driver.name, err)
			}

			var filteredTypes []string
			for chunk := range streamCh {
				if chunk.Data != nil {
					record := chunk.Data
					typeCol := record.Column(3).(*array.String)
					for i := 0; i < typeCol.Len(); i++ {
						if typeCol.IsValid(i) {
							filteredTypes = append(filteredTypes, typeCol.Value(i))
						}
					}
				}
			}

			// Only validate for SQLite (where we know filtering works)
			if driver.driverName == "adbc_driver_sqlite" {
				// Verify all returned types are in the filter list
				allowedTypes := map[string]bool{"table": true, "TABLE": true, "BASE TABLE": true}
				for _, tableType := range filteredTypes {
					if !allowedTypes[tableType] {
						t.Errorf("Table type filter test failed for %s: expected only allowed table types, but got '%s'", driver.name, tableType)
					}
				}
			}

			t.Logf("Filtered table types for %s: %v", driver.name, filteredTypes)
		})
	}
}

// Mock implementation of GetTables command
type mockGetTables struct {
	catalog               *string
	dbSchemaFilterPattern *string
	tableNameFilterPattern *string
	tableTypes            []string
	includeSchema         bool
}

func (m *mockGetTables) GetCatalog() *string {
	return m.catalog
}

func (m *mockGetTables) GetDBSchemaFilterPattern() *string {
	return m.dbSchemaFilterPattern
}

func (m *mockGetTables) GetTableNameFilterPattern() *string {
	return m.tableNameFilterPattern
}

func (m *mockGetTables) GetTableTypes() []string {
	return m.tableTypes
}

func (m *mockGetTables) GetIncludeSchema() bool {
	return m.includeSchema
}

func TestGetFlightInfoTables(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			ctx := context.Background()

			// Create a mock GetTables command
			cmd := &mockGetTables{
				catalog:               nil,
				dbSchemaFilterPattern: nil,
				tableNameFilterPattern: nil,
				tableTypes:            nil,
				includeSchema:         false,
			}

			// Create a mock flight descriptor
			desc := &flight.FlightDescriptor{
				Type: pb.FlightDescriptor_CMD,
				Cmd:  []byte("test command"),
			}

			// Call GetFlightInfoTables
			flightInfo, err := server.GetFlightInfoTables(ctx, cmd, desc)
			if err != nil {
				t.Fatalf("GetFlightInfoTables failed for %s: %v", driver.name, err)
			}

			// Verify flight info structure
			if flightInfo == nil {
				t.Fatalf("Expected FlightInfo object for %s, got nil", driver.name)
			}

			if len(flightInfo.Endpoint) != 1 {
				t.Errorf("Expected 1 endpoint for %s, got %d", driver.name, len(flightInfo.Endpoint))
			}

			if flightInfo.FlightDescriptor != desc {
				t.Errorf("FlightDescriptor mismatch for %s", driver.name)
			}

			// Verify schema is serialized Tables schema
			if flightInfo.Schema == nil {
				t.Errorf("Expected schema in FlightInfo for %s, got nil", driver.name)
			}

			// Deserialize and verify schema
			deserializedSchema, err := flight.DeserializeSchema(flightInfo.Schema, memory.DefaultAllocator)
			if err != nil {
				t.Errorf("Failed to deserialize schema for %s: %v", driver.name, err)
			} else {
				expectedSchema := schema_ref.Tables
				if !deserializedSchema.Equal(expectedSchema) {
					t.Errorf("Schema mismatch for %s.\nExpected: %v\nGot: %v", driver.name, expectedSchema, deserializedSchema)
				}
			}

			t.Logf("GetFlightInfoTables test passed for %s", driver.name)
		})
	}
}

// func TestDoGetCatalogs_ErrorHandling(t *testing.T) {
// 	// Create server with nil database to test error handling
// 	server := &DummyFlightSQLServer{
// 		db: nil,
// 	}
// 	server.Alloc = memory.DefaultAllocator

// 	ctx := context.Background()

// 	// This should fail when trying to dereference nil database
// 	_, _, err := server.DoGetCatalogs(ctx)
// 	if err == nil {
// 		t.Error("Expected DoGetCatalogs to fail with nil database, but it succeeded")
// 	}
// }

// func TestDoGetCatalogs_StreamClosure(t *testing.T) {
// 	drivers := getTestDrivers(t)

// 	for _, driver := range drivers {
// 		t.Run(driver.name, func(t *testing.T) {
// 			server, cleanup := setupTestServer(t, driver)
// 			defer cleanup()

// 			ctx := context.Background()

// 			// Call DoGetCatalogs
// 			_, streamCh, err := server.DoGetCatalogs(ctx)
// 			if err != nil {
// 				t.Fatalf("DoGetCatalogs failed for %s: %v", driver.name, err)
// 			}

// 			// Consume all chunks and verify channel is closed
// 			chunkCount := 0
// 			for chunk := range streamCh {
// 				chunkCount++
// 				if chunk.Data != nil {
// 					chunk.Data.Release() // Clean up records
// 				}
// 			}

// 			// Verify channel is closed by attempting to read from it
// 			select {
// 			case _, ok := <-streamCh:
// 				if ok {
// 					t.Errorf("Stream channel should be closed but is still open for %s", driver.name)
// 				}
// 			default:
// 				// Channel is closed as expected
// 			}

// 			t.Logf("Processed %d chunks from catalog stream for %s", chunkCount, driver.name)
// 		})
// 	}
// }

// func TestDoGetDBSchemas(t *testing.T) {
// 	drivers := getTestDrivers(t)

// 	for _, driver := range drivers {
// 		t.Run(driver.name, func(t *testing.T) {
// 			server, cleanup := setupTestServer(t, driver)
// 			defer cleanup()

// 			ctx := context.Background()

// 			// Create a mock GetDBSchemas command
// 			cmd := &mockGetDBSchemas{}

// 			// Call DoGetDBSchemas
// 			schema, streamCh, err := server.DoGetDBSchemas(ctx, cmd)
// 			if err != nil {
// 				t.Fatalf("DoGetDBSchemas failed for %s: %v", driver.name, err)
// 			}

// 			// Verify schema matches expected schema
// 			expectedSchema := schema_ref.DBSchemas
// 			if !schema.Equal(expectedSchema) {
// 				t.Errorf("Schema mismatch for %s.\nExpected: %v\nGot: %v", driver.name, expectedSchema, schema)
// 			}

// 			// Collect all records from the stream
// 			var records []arrow.Record
// 			for chunk := range streamCh {
// 				if chunk.Data != nil {
// 					records = append(records, chunk.Data)
// 				}
// 			}

// 			// For this test, we don't expect specific schemas to exist,
// 			// but the operation should complete successfully
// 			t.Logf("Found %d schema records for %s", len(records), driver.name)
// 		})
// 	}
// }

// // Mock implementation of GetDBSchemas command
// type mockGetDBSchemas struct{}

// func (m *mockGetDBSchemas) GetCatalog() *string {
// 	return nil
// }

// func (m *mockGetDBSchemas) GetDBSchemaFilterPattern() *string {
// 	return nil
// }

// func TestDummyFlightSQLServer_Creation(t *testing.T) {
// 	// Test the constructor function (uses default SQLite)
// 	server, err := NewDummyFlightSQLServer()
// 	if err != nil {
// 		t.Fatalf("NewDummyFlightSQLServer failed: %v", err)
// 	}

// 	if server == nil {
// 		t.Fatal("Expected server instance, got nil")
// 	}

// 	if server.Alloc == nil {
// 		t.Error("Expected allocator to be set")
// 	}

// 	if server.db == nil {
// 		t.Error("Expected database to be set")
// 	}

// 	// Clean up
// 	if server.db != nil {
// 		(*server.db).Close()
// 	}
// }
