package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow/array"
	// "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
)

func main() {
	var drv drivermgr.Driver

	db, err := drv.NewDatabase(map[string]string{
		"driver":     "duckdb",
		"entrypoint": "duckdb_adbc_init",
		"path":       ":memory:",
	})

	defer db.Close()

	conn, err := db.Open(context.Background())
	if err != nil {
		// handle error
	}
	defer conn.Close()

	reader, _ := conn.GetObjects(context.Background(), adbc.ObjectDepthTables, nil, nil, nil, nil, nil)

	fmt.Printf("%s", reader.Schema().String())

	for reader.Next() {
		rec := reader.Record()

		nameCol := rec.Column(0).(*array.String)
		schemasCol := rec.Column(1).(*array.List)
		catalogSchemasValues := schemasCol.ListValues().(*array.Struct)
		schemaNameCol := catalogSchemasValues.Field(0).(*array.String)

		for i := 0; i < int(rec.NumRows()); i++ {
			nameVal := nameCol.Value(i)

			start := schemasCol.Offsets()[i]
			end := schemasCol.Offsets()[i]

			for j := start; j < end; j++ {
				fmt.Printf(schemaNameCol.Value(int(j)))
			}

			fmt.Printf("Row %d, name = %s\n", i, nameVal)
		}
	}

	// if err := reader.Err(); err != nil {
	// 	log.Fatalf("reader error: %v", err)
	// }

	// nameCol := rec.Column(1).(*array.String)

	// statement, err := conn.NewStatement()
	// if err != nil {
	// 	log.Fatalf("open conn: %v", err)
	// }
	// defer statement.Close()

	// statement.SetSqlQuery("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)")
	// _, err = statement.ExecuteUpdate(context.Background())
	// if err != nil {
	// 	log.Fatalf("create table: %v", err)
	// }

	// statement.SetSqlQuery("INSERT INTO people (name) VALUES ('Alice'), ('Bob')")
	// _, err = statement.ExecuteUpdate(context.Background())
	// if err != nil {
	// 	log.Fatalf("insert: %v", err)
	// }

	// Run a simple query
	// statement.SetSqlQuery("SELECT id, name FROM people ORDER BY id")
	// reader, _, err := statement.ExecuteQuery(context.Background())

	// if err != nil {
	// 	log.Fatalf("query: %v", err)
	// }
	// defer reader.Release()

	// fmt.Println("Query results:")

	// for reader.Next() {
	// 	rec := reader.Record()

	// 	idCol := rec.Column(0).(*array.Int64)
	// 	nameCol := rec.Column(1).(*array.String)

	// 	for i := 0; i < int(rec.NumRows()); i++ {
	// 		idVal := idCol.Value(i)
	// 		nameVal := nameCol.Value(i)
	// 		fmt.Printf("Row %d: id = %d, name = %s\n", i, idVal, nameVal)
	// 	}
	// }

	// if err := reader.Err(); err != nil {
	// 	log.Fatalf("reader error: %v", err)
	// }
}
