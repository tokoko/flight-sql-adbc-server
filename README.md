# FlightSQL ADBC Server

A Go-based Apache Arrow FlightSQL server implementation demonstrating database connectivity using ADBC (Arrow Database Connectivity) drivers. This project showcases the integration between Apache Arrow's FlightSQL protocol and various database backends through ADBC.

## Architecture Overview

### Core Components

- **FlightSQL Server** (`cmd/server/main.go`): Main server implementing the Arrow Flight SQL protocol
- **Client Examples** (`examples/`): Demonstration clients for FlightSQL and DuckDB connectivity
- **Database Integration**: Uses ADBC driver manager with SQLite backend support

### Technology Stack

- **Apache Arrow Go** (v18.4.1): Arrow format and Flight protocol
- **Apache Arrow ADBC Go** (v1.8.0): Database connectivity abstraction
- **Supported Drivers**: SQLite (server), DuckDB (examples), FlightSQL (client)
- **Environment**: Pixi-managed dependencies with conda-forge packages

## FlightSQL Protocol Interface Coverage

This server implements a **subset** of the full FlightSQL specification. Below is the complete coverage analysis:

### ✅ Implemented Methods

| Category | Method | Implementation | Notes |
|----------|--------|----------------|-------|
| **Metadata** | `GetFlightInfoCatalogs` | ✅ | `cmd/server/main.go:54` |
| **Metadata** | `DoGetCatalogs` | ✅ | `cmd/server/main.go:64` |
| **Metadata** | `GetFlightInfoSchemas` | ✅ | `cmd/server/main.go:97` |
| **Metadata** | `DoGetDBSchemas` | ✅ | `cmd/server/main.go:107` |
| **Metadata** | `GetFlightInfoTables` | ✅ | `cmd/server/main.go:324` |
| **Metadata** | `DoGetTables` | ✅ | `cmd/server/main.go:334` |
| **Query** | `GetFlightInfoStatement` | ✅ | `cmd/server/main.go:169` |
| **Query** | `GetSchemaStatement` | ✅ | `cmd/server/main.go:230` |
| **Query** | `DoGetStatement` | ✅ | `cmd/server/main.go:269` |

### ❌ Not Implemented Methods

| Category | Method | Description |
|----------|--------|-------------|
| **Metadata** | `GetCrossReference` | Foreign key relationships between tables |
| **Metadata** | `GetExportedKeys` | Foreign keys exported by a table |
| **Metadata** | `GetImportedKeys` | Foreign keys imported by a table |
| **Metadata** | `GetPrimaryKeys` | Primary key information |
| **Metadata** | `GetSqlInfo` | Server capability and configuration info (including Substrait support) |
| **Metadata** | `GetTableTypes` | Available table types |
| **Query** | `CreatePreparedStatement` | Prepared statement creation |
| **Query** | `ClosePreparedStatement` | Prepared statement cleanup |
| **Query** | `PreparedStatementQuery` | Execute prepared SELECT statements |
| **Query** | `StatementUpdate` | Execute DML statements (INSERT/UPDATE/DELETE) |
| **Query** | `PreparedStatementUpdate` | Execute prepared DML statements |
| **Query** | `StatementIngest` | Bulk data ingestion |
| **Session** | `SetSessionOptions` | Configure session parameters |
| **Session** | `GetSessionOptions` | Retrieve session configuration |
| **Session** | `CloseSession` | Session termination |
| **Transaction** | `BeginTransaction` | Start database transactions |
| **Transaction** | `BeginSavepoint` | Create transaction savepoints |
| **Transaction** | `EndTransaction` | Commit/rollback transactions |
| **Transaction** | `EndSavepoint` | Release/rollback savepoints |
| **Substrait** | `GetFlightInfoSubstraitPlan` | Execute Substrait plans with FlightInfo response |
| **Substrait** | `GetSchemaSubstraitPlan` | Get schema for Substrait plan execution |
| **Substrait** | `DoPutCommandSubstraitPlan` | Execute Substrait plan updates (DML) |
| **Substrait** | `CreatePreparedSubstraitPlan` | Create prepared statements from Substrait plans |
| **Substrait** | `PollFlightInfoSubstraitPlan` | Poll for Substrait plan execution status |

### Implementation Details

**Current Capabilities:**
- ✅ Basic catalog and schema metadata retrieval
- ✅ Table listing with filtering support
- ✅ SQL query execution with schema inference
- ✅ Query schema introspection without data execution
- ✅ Arrow-formatted result streaming
- ✅ SQLite backend integration via ADBC

**Schema Resolution Approach:**

The `GetSchemaStatement` method uses an efficient schema-only query technique:
- Wraps the original query with `WHERE 1=0` to get schema without executing data
- Example: `SELECT * FROM (original_query) WHERE 1=0`
- Supports complex queries including subqueries, joins, and functions
- Returns serialized Arrow schema via `flight.SchemaResult`
- Comprehensive test coverage for SQLite and DuckDB backends

**Missing Features:**
- ❌ Prepared statements and parameterized queries
- ❌ Transaction management
- ❌ Session management
- ❌ Advanced metadata operations (keys, constraints, SQL info)
- ❌ Substrait plan execution and integration
- ❌ Data modification operations (INSERT/UPDATE/DELETE)
- ❌ Bulk ingestion capabilities
- ❌ Server capability introspection

## Getting Started

### Prerequisites

Ensure you have Pixi installed for dependency management.

### Environment Setup

```bash
# Initialize the pixi environment (auto-installs all dependencies)
pixi shell

# Dependencies include:
# - Go toolchain, GCC/G++ compilers
# - libadbc-driver-sqlite, libadbc-driver-flightsql, libduckdb
# - AWS CLI, Node.js
```

### Running the Server

```bash
# Build and start the FlightSQL server
go run cmd/server/main.go

# Server will listen on localhost:33333
# Creates/uses 'bla.db' SQLite database in project root
```

### Running Client Examples

```bash
# Connect to FlightSQL server using ADBC FlightSQL driver
go run examples/flightsql_client.go

# Direct ADBC usage with DuckDB (for comparison)
go run examples/duckdb_client.go
```

### Development Commands

```bash
go build          # Build current package
go test ./...     # Run all tests
go mod tidy       # Clean up dependencies
go fmt ./...      # Format code
go vet ./...      # Static analysis

# Run specific test suites
go test ./cmd/server -v -run TestGetSchemaStatement  # Schema introspection tests
go test ./cmd/server -v -run TestDoGetStatement      # Query execution tests
```

## Project Structure

```
├── cmd/server/main.go          # FlightSQL server implementation
├── examples/
│   ├── flightsql_client.go     # FlightSQL client example
│   └── duckdb_client.go        # DuckDB ADBC client example
├── pixi.toml                   # Pixi environment configuration
├── go.mod                      # Go module dependencies
└── CLAUDE.md                   # Development instructions
```

## Key Dependencies

**Runtime:**
- `github.com/apache/arrow-go/v18`: Arrow format and Flight protocol
- `github.com/apache/arrow-adbc/go`: ADBC database connectivity

**System Libraries (via pixi):**
- `libadbc-driver-sqlite`: SQLite ADBC driver
- `libadbc-driver-flightsql`: FlightSQL ADBC driver
- `libduckdb`: DuckDB database engine
- `gcc`/`gxx`: Required for CGO compilation

## Extending the Server

To implement additional FlightSQL methods:

1. **Metadata Methods**: Add method implementations following the pattern in `GetFlightInfoCatalogs`/`DoGetCatalogs`
2. **Query Operations**: Extend statement handling similar to `GetFlightInfoStatement`/`DoGetStatement`
3. **Schema Compliance**: Use Arrow schema references from `schema_ref` package
4. **ADBC Integration**: Leverage the existing `adbc.Database` connection for data operations

See the [FlightSQL specification](https://arrow.apache.org/docs/format/FlightSql.html) for complete method signatures and expected behavior.

## Contributing

This is a demonstration project showcasing FlightSQL and ADBC integration. Contributions to expand protocol coverage or add new database backends are welcome.