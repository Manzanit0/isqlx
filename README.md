# isqlx

A limited wrapper around `jmoiron/sqlx` with instrumentation through OTEL tracers.

The idea is that you can add it as a drop-in replacement for `jmoiron/sqlx` and
if you're leveraging OTEL already, then any database query should be
automatically instrumented out-of-the box leveraging [OTEL trace semantics](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/README.md)

Always work in progress.

## how-to

Initialise the library with the config (in the example retrieved from env vars):

```go
// You need to have configured the OTEL exporter, tracer and whatnot already; so
// we'll assume that's good to go. To get the tracer just run:
tracer := otel.Tracer(serviceName)

// Then parse the config from the environment.
port, err := strconv.Atoi(os.Getenv("PGPORT"))
if err != nil {
    log.Fatalf("parse db port from env var PGPORT: %s", err.Error())
}

config := isqlx.DBConfig{
    Host:     os.Getenv("PGHOST"),
    Port:     port,
    User:     os.Getenv("PGUSER"),
    Password: os.Getenv("PGPASSWORD"),
    Name:     os.Getenv("PGDATABASE"),
}

// And with the tracer and the config, the database wrapper should starteable.
dbx, err := isqlx.NewDBXFromConfig("pgx", &config, tracer)
if err != nil {
    log.Fatalf("open postgres connection: %s", err.Error())
}

// The library leaks the jmoiron/sqlx abstraction through the GetSQLX()
// function, which means you never need to add sqlx as a dependency. Leveraging
// this, we can just use the Ping() function that way.
err = dbx.GetSQLX().DB.Ping()
if err != nil {
    log.Fatalf("ping postgres connection: %s", err.Error())
}
```

And then just use it as you would use `jmoiron/sqlx`, you can query your
database, producing sub-spans to the existing trace along the way:

```go
type Expense struct {
	ID          int64     `db:"id"`
	Amount      int32     `db:"amount"`
	UserEmail   string    `db:"user_email"`
}

var expenses []Expense
err := dbx.SelectContext(ctx, &expenses, `SELECT id, amount FROM expenses WHERE user_email = $1`, email)
if err != nil {
    log.Fatalf("unable to execute query: %s", err.Error())
}
```
