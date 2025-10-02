package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// DummyFlightSQLServer implements the FlightSQLServer interface
type DummyFlightSQLServer struct {
	flightsql.BaseServer
	db      *adbc.Database
	queries map[string]string // map of statement handle to query
}

func NewDummyFlightSQLServer() (*DummyFlightSQLServer, error) {
	drv := &drivermgr.Driver{}

	db, err := drv.NewDatabase(map[string]string{
		"driver":          "adbc_driver_sqlite",
		adbc.OptionKeyURI: "bla.db",
	})

	if err != nil {
		fmt.Println(err)
	}

	ret := &DummyFlightSQLServer{
		db:      &db,
		queries: make(map[string]string),
	}

	ret.Alloc = memory.DefaultAllocator
	// for k, v := range SqlInfoResultMap() {
	// 	ret.RegisterSqlInfo(flightsql.SqlInfo(k), v)
	// }
	return ret, nil
}

func (s *DummyFlightSQLServer) GetFlightInfoCatalogs(context context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema_ref.Catalogs, s.Alloc),
	}, nil
}

func (s *DummyFlightSQLServer) DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := schema_ref.Catalogs

	if s.db == nil {
		return nil, nil, fmt.Errorf("database is not initialized")
	}

	db := *s.db
	conn, err := db.Open(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	reader, err := conn.GetObjects(context.Background(), adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)

	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk, 1)

	for reader.Next() {
		rec := reader.Record()
		record := array.NewRecord(schema, []arrow.Array{rec.Column(0)}, 0)
		ch <- flight.StreamChunk{Data: record}
	}

	close(ch)

	return schema, ch, nil
}

func (s *DummyFlightSQLServer) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema_ref.DBSchemas, s.Alloc),
	}, nil
}

func (s *DummyFlightSQLServer) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := schema_ref.DBSchemas

	db := *s.db
	conn, err := db.Open(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	reader, err := conn.GetObjects(ctx, adbc.ObjectDepthDBSchemas, cmd.GetCatalog(), cmd.GetDBSchemaFilterPattern(), nil, nil, nil)

	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)

	go func() {
		defer close(ch)
		defer reader.Release()

		for reader.Next() {
			rec := reader.RecordBatch()

			catalogNameBuilder := array.NewStringBuilder(s.Alloc)
			dbSchemaNameBuilder := array.NewStringBuilder(s.Alloc)

			catalogNameCol := rec.Column(0).(*array.String)
			schemasCol := rec.Column(1).(*array.List)
			catalogSchemasValues := schemasCol.ListValues().(*array.Struct)
			schemaNameCol := catalogSchemasValues.Field(0).(*array.String)

			for i := 0; i < int(rec.NumRows()); i++ {
				catalogName := catalogNameCol.Value(i)

				start := schemasCol.Offsets()[i]
				end := schemasCol.Offsets()[i+1] // Fix: use i+1 instead of i

				for j := start; j < end; j++ {
					schemaName := schemaNameCol.Value(int(j))
					catalogNameBuilder.Append(catalogName)
					dbSchemaNameBuilder.Append(schemaName)
				}
			}

			cols := []arrow.Array{
				catalogNameBuilder.NewArray(),
				dbSchemaNameBuilder.NewArray(),
			}

			catalogNameBuilder.Release()
			dbSchemaNameBuilder.Release()

			record := array.NewRecordBatch(schema, cols, int64(cols[0].Len()))
			ch <- flight.StreamChunk{Data: record}
		}
	}()

	return schema, ch, nil
}

func (s *DummyFlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("Received query:", cmd.GetQuery())

	if s.db == nil {
		return nil, fmt.Errorf("database is not initialized")
	}

	// Generate a unique handle for this query
	handleBytes := make([]byte, 16)
	_, err := rand.Read(handleBytes)
	if err != nil {
		return nil, err
	}
	handle := hex.EncodeToString(handleBytes)

	// Store the original query for later retrieval
	s.queries[handle] = cmd.GetQuery()

	db := *s.db
	conn, err := db.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// Wrap the original query with WHERE 1=0 to get schema without executing the full query
	schemaQuery := fmt.Sprintf("SELECT * FROM (%s) WHERE 1=0", cmd.GetQuery())
	err = stmt.SetSqlQuery(schemaQuery)
	if err != nil {
		return nil, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	schema := reader.Schema()

	// Create a ticket with the statement handle
	ticket, err := flightsql.CreateStatementQueryTicket([]byte(handle))
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticket},
		}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, s.Alloc),
	}, nil
}

func (s *DummyFlightSQLServer) GetSchemaStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	fmt.Println("Getting schema for query:", cmd.GetQuery())

	if s.db == nil {
		return nil, fmt.Errorf("database is not initialized")
	}

	db := *s.db
	conn, err := db.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// Wrap the original query with WHERE 1=0 to get schema without executing the full query
	schemaQuery := fmt.Sprintf("SELECT * FROM (%s) WHERE 1=0", cmd.GetQuery())
	err = stmt.SetSqlQuery(schemaQuery)
	if err != nil {
		return nil, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	schema := reader.Schema()
	return &flight.SchemaResult{
		Schema: flight.SerializeSchema(schema, s.Alloc),
	}, nil
}

func (s *DummyFlightSQLServer) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	fmt.Println("Executing statement for ticket")

	// Get the statement handle and look up the query
	handle := string(cmd.GetStatementHandle())
	query, exists := s.queries[handle]
	if !exists {
		return nil, nil, fmt.Errorf("unknown statement handle: %s", handle)
	}

	fmt.Println("Query:", query)

	if s.db == nil {
		return nil, nil, fmt.Errorf("database is not initialized")
	}

	db := *s.db
	conn, err := db.Open(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, nil, err
	}
	defer stmt.Close()

	err = stmt.SetSqlQuery(query)
	if err != nil {
		return nil, nil, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, nil, err
	}

	schema := reader.Schema()
	ch := make(chan flight.StreamChunk)

	go func() {
		defer close(ch)
		defer reader.Release()
		for reader.Next() {
			rec := reader.RecordBatch()
			rec.Retain()
			ch <- flight.StreamChunk{Data: rec}
		}
	}()

	return schema, ch, nil
}

func (s *DummyFlightSQLServer) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema_ref.Tables, s.Alloc),
	}, nil
}

func (s *DummyFlightSQLServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := schema_ref.Tables

	db := *s.db
	conn, err := db.Open(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	// Use GetObjects with table depth to get table metadata
	reader, err := conn.GetObjects(ctx, adbc.ObjectDepthTables, cmd.GetCatalog(), cmd.GetDBSchemaFilterPattern(), cmd.GetTableNameFilterPattern(), nil, cmd.GetTableTypes())
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)

	go func() {
		defer close(ch)
		defer reader.Release()

		for reader.Next() {
			rec := reader.RecordBatch()

			catalogNameBuilder := array.NewStringBuilder(s.Alloc)
			dbSchemaNameBuilder := array.NewStringBuilder(s.Alloc)
			tableNameBuilder := array.NewStringBuilder(s.Alloc)
			tableTypeBuilder := array.NewStringBuilder(s.Alloc)

			catalogNameCol := rec.Column(0).(*array.String)
			schemasCol := rec.Column(1).(*array.List)
			catalogSchemasValues := schemasCol.ListValues().(*array.Struct)
			schemaNameCol := catalogSchemasValues.Field(0).(*array.String)
			tablesCol := catalogSchemasValues.Field(1).(*array.List)
			schemaTablesValues := tablesCol.ListValues().(*array.Struct)
			tableNameCol := schemaTablesValues.Field(0).(*array.String)
			tableTypeCol := schemaTablesValues.Field(1).(*array.String)

			for i := 0; i < int(rec.NumRows()); i++ {
				catalogName := catalogNameCol.Value(i)

				schemaStart := schemasCol.Offsets()[i]
				schemaEnd := schemasCol.Offsets()[i+1]

				for j := schemaStart; j < schemaEnd; j++ {
					schemaName := schemaNameCol.Value(int(j))

					tableStart := tablesCol.Offsets()[j]
					tableEnd := tablesCol.Offsets()[j+1]

					for k := tableStart; k < tableEnd; k++ {
						tableName := tableNameCol.Value(int(k))
						tableType := tableTypeCol.Value(int(k))

						catalogNameBuilder.Append(catalogName)
						dbSchemaNameBuilder.Append(schemaName)
						tableNameBuilder.Append(tableName)
						tableTypeBuilder.Append(tableType)
					}
				}
			}

			cols := []arrow.Array{
				catalogNameBuilder.NewArray(),
				dbSchemaNameBuilder.NewArray(),
				tableNameBuilder.NewArray(),
				tableTypeBuilder.NewArray(),
			}

			catalogNameBuilder.Release()
			dbSchemaNameBuilder.Release()
			tableNameBuilder.Release()
			tableTypeBuilder.Release()

			record := array.NewRecordBatch(schema, cols, int64(cols[0].Len()))
			ch <- flight.StreamChunk{Data: record}
		}
	}()

	return schema, ch, nil
}

func main() {
	addr := "localhost"
	port := 33333

	server := flight.NewServerWithMiddleware(nil)

	s, _ := NewDummyFlightSQLServer()
	server.RegisterFlightService(flightsql.NewFlightServer(s))
	server.Init(net.JoinHostPort(addr, strconv.Itoa(port)))
	server.SetShutdownOnSignals(os.Interrupt, os.Kill)

	fmt.Printf("Dummy FlightSQL server listening on %s\n", addr)

	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}
