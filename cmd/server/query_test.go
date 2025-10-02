package main

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Mock StatementQuery implementation
type mockStatementQuery struct {
	query string
}

func (m *mockStatementQuery) GetQuery() string {
	return m.query
}

func (m *mockStatementQuery) GetTransactionId() []byte {
	return nil
}

func TestDoGetStatement(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			// Set up test data
			setupTestData(t, server)

			ctx := context.Background()

			// First create a FlightInfo to get a valid ticket
			query := "SELECT id, name FROM test_table ORDER BY id"
			cmd := &mockStatementQuery{query: query}
			desc := &flight.FlightDescriptor{
				Type: 0, // CMD type
				Cmd:  []byte("test-command"),
			}

			flightInfo, err := server.GetFlightInfoStatement(ctx, cmd, desc)
			if err != nil {
				t.Fatalf("GetFlightInfoStatement failed for %s: %v", driver.name, err)
			}

			// Extract the statement ticket
			ticket := flightInfo.Endpoint[0].Ticket
			statementTicket, err := flightsql.GetStatementQueryTicket(ticket)
			if err != nil {
				t.Fatalf("Failed to parse statement ticket for %s: %v", driver.name, err)
			}

			// Call DoGetStatement
			schema, streamCh, err := server.DoGetStatement(ctx, statementTicket)
			if err != nil {
				t.Fatalf("DoGetStatement failed for %s: %v", driver.name, err)
			}

			// Verify schema
			if schema == nil {
				t.Fatalf("Expected schema, got nil for %s", driver.name)
			}

			// Collect all records from the stream
			var records []arrow.RecordBatch
			for chunk := range streamCh {
				if chunk.Data != nil {
					records = append(records, chunk.Data)
				}
			}

			// Verify we got records
			if len(records) == 0 {
				t.Errorf("Expected at least one record for %s, got none", driver.name)
				return
			}

			// Verify first record structure
			record := records[0]
			if record.NumCols() != 2 {
				t.Errorf("Expected 2 columns for %s, got %d", driver.name, record.NumCols())
			}

			// Verify name column (should be consistent across drivers)
			nameCol, ok := record.Column(1).(*array.String)
			if !ok {
				t.Errorf("Expected string array for name column in %s, got %T", driver.name, record.Column(1))
				return
			}

			// Verify we have the expected number of rows
			if nameCol.Len() != 3 {
				t.Errorf("Expected 3 rows for %s, got %d", driver.name, nameCol.Len())
			}

			// Verify first row values (handle different integer types for ID column)
			if nameCol.Len() > 0 {
				firstName := nameCol.Value(0)

				if firstName != "test1" {
					t.Errorf("Expected first name to be 'test1' for %s, got '%s'", driver.name, firstName)
				}

				// Check ID value based on driver type
				idCol := record.Column(0)
				switch col := idCol.(type) {
				case *array.Int64:
					if col.Value(0) != 1 {
						t.Errorf("Expected first id to be 1 for %s, got %d", driver.name, col.Value(0))
					}
				case *array.Int32:
					if col.Value(0) != 1 {
						t.Errorf("Expected first id to be 1 for %s, got %d", driver.name, col.Value(0))
					}
				default:
					t.Errorf("Expected integer array for id column in %s, got %T", driver.name, idCol)
				}
			}

			t.Logf("Successfully executed query and got %d records for %s", len(records), driver.name)
		})
	}
}

func TestGetSchemaStatement(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			// Set up test data
			setupTestData(t, server)

			ctx := context.Background()

			// Test basic query
			query := "SELECT id, name, value FROM test_table"
			cmd := &mockStatementQuery{query: query}
			desc := &flight.FlightDescriptor{
				Type: 0,
				Cmd:  []byte("test-command"),
			}

			schemaResult, err := server.GetSchemaStatement(ctx, cmd, desc)
			if err != nil {
				t.Fatalf("GetSchemaStatement failed for %s: %v", driver.name, err)
			}

			if schemaResult == nil {
				t.Fatalf("Expected schema result, got nil for %s", driver.name)
			}

			// Deserialize the schema
			schema, err := flight.DeserializeSchema(schemaResult.Schema, memory.DefaultAllocator)
			if err != nil {
				t.Fatalf("Failed to deserialize schema for %s: %v", driver.name, err)
			}

			// Verify schema structure
			if schema.NumFields() != 3 {
				t.Errorf("Expected 3 fields for %s, got %d", driver.name, schema.NumFields())
			}

			// Check field names
			expectedFields := []string{"id", "name", "value"}
			for i, expectedName := range expectedFields {
				if i < schema.NumFields() {
					field := schema.Field(i)
					if field.Name != expectedName {
						t.Errorf("Expected field %d to be '%s' for %s, got '%s'", i, expectedName, driver.name, field.Name)
					}
				}
			}

			t.Logf("Successfully got schema with %d fields for %s", schema.NumFields(), driver.name)
		})
	}
}

func TestGetSchemaStatement_SelectStar(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			// Set up test data
			setupTestData(t, server)

			ctx := context.Background()

			// Test SELECT * query
			query := "SELECT * FROM test_table"
			cmd := &mockStatementQuery{query: query}
			desc := &flight.FlightDescriptor{
				Type: 0,
				Cmd:  []byte("test-command"),
			}

			schemaResult, err := server.GetSchemaStatement(ctx, cmd, desc)
			if err != nil {
				t.Fatalf("GetSchemaStatement failed for %s: %v", driver.name, err)
			}

			if schemaResult == nil {
				t.Fatalf("Expected schema result, got nil for %s", driver.name)
			}

			// Deserialize the schema
			schema, err := flight.DeserializeSchema(schemaResult.Schema, memory.DefaultAllocator)
			if err != nil {
				t.Fatalf("Failed to deserialize schema for %s: %v", driver.name, err)
			}

			// Should have all columns from test_table
			if schema.NumFields() != 3 {
				t.Errorf("Expected 3 fields for %s, got %d", driver.name, schema.NumFields())
			}

			t.Logf("Successfully got schema for SELECT * with %d fields for %s", schema.NumFields(), driver.name)
		})
	}
}

func TestGetSchemaStatement_WithOrderBy(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			// Set up test data
			setupTestData(t, server)

			ctx := context.Background()

			// Test query with ORDER BY
			query := "SELECT id, name FROM test_table ORDER BY id DESC"
			cmd := &mockStatementQuery{query: query}
			desc := &flight.FlightDescriptor{
				Type: 0,
				Cmd:  []byte("test-command"),
			}

			schemaResult, err := server.GetSchemaStatement(ctx, cmd, desc)
			if err != nil {
				t.Fatalf("GetSchemaStatement failed for %s: %v", driver.name, err)
			}

			if schemaResult == nil {
				t.Fatalf("Expected schema result, got nil for %s", driver.name)
			}

			// Deserialize the schema
			schema, err := flight.DeserializeSchema(schemaResult.Schema, memory.DefaultAllocator)
			if err != nil {
				t.Fatalf("Failed to deserialize schema for %s: %v", driver.name, err)
			}

			// Should have 2 columns
			if schema.NumFields() != 2 {
				t.Errorf("Expected 2 fields for %s, got %d", driver.name, schema.NumFields())
			}

			// Check field names
			expectedFields := []string{"id", "name"}
			for i, expectedName := range expectedFields {
				if i < schema.NumFields() {
					field := schema.Field(i)
					if field.Name != expectedName {
						t.Errorf("Expected field %d to be '%s' for %s, got '%s'", i, expectedName, driver.name, field.Name)
					}
				}
			}

			t.Logf("Successfully got schema for ORDER BY query with %d fields for %s", schema.NumFields(), driver.name)
		})
	}
}

func TestGetSchemaStatement_ErrorHandling(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name+"_InvalidQuery", func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			ctx := context.Background()

			// Test with invalid SQL
			query := "SELECT * FROM non_existent_table"
			cmd := &mockStatementQuery{query: query}
			desc := &flight.FlightDescriptor{
				Type: 0,
				Cmd:  []byte("test-command"),
			}

			_, err := server.GetSchemaStatement(ctx, cmd, desc)
			if err == nil {
				t.Errorf("Expected GetSchemaStatement to fail with invalid query for %s, but it succeeded", driver.name)
			}

			t.Logf("Correctly failed with invalid query for %s: %v", driver.name, err)
		})

		t.Run(driver.name+"_InvalidSyntax", func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			ctx := context.Background()

			// Test with invalid SQL syntax
			query := "SELECT * FROM"
			cmd := &mockStatementQuery{query: query}
			desc := &flight.FlightDescriptor{
				Type: 0,
				Cmd:  []byte("test-command"),
			}

			_, err := server.GetSchemaStatement(ctx, cmd, desc)
			if err == nil {
				t.Errorf("Expected GetSchemaStatement to fail with invalid syntax for %s, but it succeeded", driver.name)
			}

			t.Logf("Correctly failed with invalid syntax for %s: %v", driver.name, err)
		})
	}

	// Test with nil database
	t.Run("NilDatabase", func(t *testing.T) {
		server := &DummyFlightSQLServer{
			db:      nil,
			queries: make(map[string]string),
		}
		server.Alloc = memory.DefaultAllocator

		ctx := context.Background()
		query := "SELECT 1"
		cmd := &mockStatementQuery{query: query}
		desc := &flight.FlightDescriptor{
			Type: 0,
			Cmd:  []byte("test-command"),
		}

		_, err := server.GetSchemaStatement(ctx, cmd, desc)
		if err == nil {
			t.Error("Expected GetSchemaStatement to fail with nil database, but it succeeded")
		}

		expectedErrorMsg := "database is not initialized"
		if !strings.Contains(err.Error(), expectedErrorMsg) {
			t.Errorf("Expected error containing '%s', got: %v", expectedErrorMsg, err)
		}
	})
}

func TestGetSchemaStatement_ComplexQueries(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name, func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			// Set up test data
			setupTestData(t, server)

			ctx := context.Background()

			// Test subquery
			query := "SELECT id, UPPER(name) as upper_name FROM (SELECT * FROM test_table WHERE id > 1) AS subq"
			cmd := &mockStatementQuery{query: query}
			desc := &flight.FlightDescriptor{
				Type: 0,
				Cmd:  []byte("test-command"),
			}

			schemaResult, err := server.GetSchemaStatement(ctx, cmd, desc)
			if err != nil {
				t.Fatalf("GetSchemaStatement failed for complex query %s: %v", driver.name, err)
			}

			if schemaResult == nil {
				t.Fatalf("Expected schema result, got nil for %s", driver.name)
			}

			// Deserialize the schema
			schema, err := flight.DeserializeSchema(schemaResult.Schema, memory.DefaultAllocator)
			if err != nil {
				t.Fatalf("Failed to deserialize schema for %s: %v", driver.name, err)
			}

			// Should have 2 columns
			if schema.NumFields() != 2 {
				t.Errorf("Expected 2 fields for %s, got %d", driver.name, schema.NumFields())
			}

			// Check field names
			if schema.NumFields() >= 2 {
				if schema.Field(0).Name != "id" {
					t.Errorf("Expected first field to be 'id' for %s, got '%s'", driver.name, schema.Field(0).Name)
				}
				if schema.Field(1).Name != "upper_name" {
					t.Errorf("Expected second field to be 'upper_name' for %s, got '%s'", driver.name, schema.Field(1).Name)
				}
			}

			t.Logf("Successfully got schema for complex query with %d fields for %s", schema.NumFields(), driver.name)
		})
	}
}

func TestDoGetStatement_ErrorHandling(t *testing.T) {
	drivers := getTestDrivers(t)

	for _, driver := range drivers {
		t.Run(driver.name+"_UnknownHandle", func(t *testing.T) {
			server, cleanup := setupTestServer(t, driver)
			defer cleanup()

			ctx := context.Background()

			// Create a mock statement ticket with unknown handle
			unknownHandle := []byte("unknown-handle-12345")
			ticketBytes, err := flightsql.CreateStatementQueryTicket(unknownHandle)
			if err != nil {
				t.Fatalf("Failed to create test ticket for %s: %v", driver.name, err)
			}

			ticket := &flight.Ticket{Ticket: ticketBytes}
			statementTicket, err := flightsql.GetStatementQueryTicket(ticket)
			if err != nil {
				t.Fatalf("Failed to parse test ticket for %s: %v", driver.name, err)
			}

			// Call DoGetStatement with unknown handle
			_, _, err = server.DoGetStatement(ctx, statementTicket)
			if err == nil {
				t.Errorf("Expected DoGetStatement to fail with unknown handle for %s, but it succeeded", driver.name)
			}

			expectedErrorMsg := "unknown statement handle"
			if !strings.Contains(err.Error(), expectedErrorMsg) {
				t.Errorf("Expected error containing '%s' for %s, got: %v", expectedErrorMsg, driver.name, err)
			}
		})
	}

	// Test with nil database
	t.Run("NilDatabase", func(t *testing.T) {
		server := &DummyFlightSQLServer{
			db:      nil,
			queries: map[string]string{"test-handle": "SELECT 1"},
		}
		server.Alloc = memory.DefaultAllocator

		ctx := context.Background()

		// Create a mock statement ticket
		ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte("test-handle"))
		if err != nil {
			t.Fatalf("Failed to create test ticket: %v", err)
		}

		ticket := &flight.Ticket{Ticket: ticketBytes}
		statementTicket, err := flightsql.GetStatementQueryTicket(ticket)
		if err != nil {
			t.Fatalf("Failed to parse test ticket: %v", err)
		}

		_, _, err = server.DoGetStatement(ctx, statementTicket)
		if err == nil {
			t.Error("Expected DoGetStatement to fail with nil database, but it succeeded")
		}

		expectedErrorMsg := "database is not initialized"
		if !strings.Contains(err.Error(), expectedErrorMsg) {
			t.Errorf("Expected error containing '%s', got: %v", expectedErrorMsg, err)
		}
	})
}
