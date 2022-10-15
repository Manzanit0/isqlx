package isqlx

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Querier is an interface which exposes functions to run queries. It's a subset
// of sqlx library functions.
type Querier interface {
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
}

// DBX is an interface to make single queries without leveraging transactions.
type DBX interface {
	Querier
	Begin(ctx context.Context) (TX, error)

	// GetSQLX is a way to escape the abstraction when needed.
	GetSQLX() *sqlx.DB
}

// TX is an interface to make queries within a database transaction.  Every
// transaction should invoke TxClose() deferred to make sure that there aren't
// any transaction leaks and that they are rolledback in case of error or panic,
// and Commit() to commit the transaction.
type TX interface {
	Querier
	Commit(ctx context.Context) error
	TxClose(ctx context.Context)
}

// TODO: if instead of taking the actual sql.DB we took the connection details,
// these could be tracked in a per-span basis.
func NewMySQLDBX(db *sql.DB, tracer trace.Tracer) DBX {
	d := sqlx.NewDb(db, "mysql")
	return &dbx{DB: d, driver: "mysql", tracer: tracer}
}

type dbx struct {
	DB     *sqlx.DB
	driver string
	tracer trace.Tracer
}

type tx struct {
	TX     *sqlx.Tx
	driver string
	tracer trace.Tracer
}

func (d *dbx) GetSQLX() *sqlx.DB {
	return d.DB
}

func (d *dbx) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return getContext(ctx, d, d.tracer, d.driver, dest, query, args)
}

func (d *dbx) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return selectContext(ctx, d, d.tracer, d.driver, dest, query, args)
}

func (d *dbx) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return namedExecContext(ctx, d, d.tracer, d.driver, query, arg)
}

func (d *dbx) Begin(_ context.Context) (TX, error) {
	t, err := d.DB.Beginx()
	if err != nil {
		return nil, err
	}

	return &tx{TX: t, driver: d.driver, tracer: d.tracer}, nil
}

func (t *tx) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return getContext(ctx, t, t.tracer, t.driver, dest, query, args...)
}

func (t *tx) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return selectContext(ctx, t, t.tracer, t.driver, dest, query, args)
}

func (t *tx) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return namedExecContext(ctx, t, t.tracer, t.driver, query, arg)
}

func (t *tx) Commit(ctx context.Context) error {
	_, span := newSpan(ctx, t.driver, "commit", t.tracer)
	defer span.End()

	err := t.TX.Commit()
	if err != nil {
		span.RecordError(err)
	}

	return err
}

// TxClose makes sure the transaction gets rolled back. It should be run within
// a `defer` statement so it can rollback transactions even in the case of
// panics.
func (t *tx) TxClose(ctx context.Context) {
	_, span := newSpan(ctx, t.driver, "rollback", t.tracer)
	defer span.End()

	if r := recover(); r != nil {
		log.Printf("recovered an error in TxClose(): %#v", r)
		_ = t.TX.Rollback()
		panic(r)
	} else {
		// Transaction leak failsafe:
		//
		// I don't check the errors here because the transaction might already
		// have been committed/rolledback. If there's an issue with the database
		// connection we'll catch it the next time that db handle gets used.
		_ = t.TX.Rollback()
	}
}

func getContext(ctx context.Context, q Querier, tracer trace.Tracer, driver string, dest interface{}, query string, args ...interface{}) error {
	ctx, span := newSpan(ctx, driver, query, tracer)
	defer span.End()

	span.addQueryParams(args)

	err := q.GetContext(ctx, dest, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			span.addAffectedRowsAttribute(0)
		} else {
			span.RecordError(err)
		}
	}

	return err
}

func selectContext(ctx context.Context, q Querier, tracer trace.Tracer, driver string, dest interface{}, query string, args ...interface{}) error {
	ctx, span := newSpan(ctx, driver, query, tracer)
	defer span.End()

	span.addQueryParams(args)

	err := q.SelectContext(ctx, dest, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			span.addAffectedRowsAttribute(0)
		} else {
			span.RecordError(err)
		}
	} else if n := getReturnedRows(dest); n != -1 {
		span.addAffectedRowsAttribute(int64(n))
	}

	return err
}

func namedExecContext(ctx context.Context, q Querier, tracer trace.Tracer, driver string, query string, arg interface{}) (sql.Result, error) {
	ctx, span := newSpan(ctx, driver, query, tracer)
	defer span.End()

	// I'm not sure if there are more use cases other than a map, but to be safe,
	// I decided to wrap it in a conditional. As we find new, let's just add them here though.
	if m, ok := arg.(map[string]interface{}); ok {
		for k, v := range m {
			span.addQueryParamAttribute(k, fmt.Sprint(v))
		}
	}

	r, err := q.NamedExecContext(ctx, query, arg)
	if err != nil {
		if err != sql.ErrNoRows {
			span.RecordError(err)
		}
	} else if n, err := r.RowsAffected(); err != nil {
		span.addAffectedRowsAttribute(n)
	}

	return r, err
}

func parseQueryOperation(query string) string {
	query = strings.ToLower(query)
	if strings.HasPrefix(query, "update") { // nolint: gocritic
		return "update"
	} else if strings.HasPrefix(query, "select") {
		return "select"
	} else if strings.HasPrefix(query, "insert") {
		return "insert"
	} else if strings.HasPrefix(query, "delete") {
		return "delete"
	} else if strings.HasPrefix(query, "commit") {
		return "commit"
	} else if strings.HasPrefix(query, "rollback") {
		return "rollback"
	}

	return "unknown"
}

// getReturnedRows extracts the amount of rows returned from dest assuming that it's
// the result of a database operation.
// @see https://goplay.tools/snippet/oKaFkTexWBk
func getReturnedRows(dest interface{}) int {
	t := reflect.TypeOf(dest)

	switch t.Kind() {
	case reflect.Slice:
		return reflect.ValueOf(dest).Len()
	case reflect.Array:
		return t.Len()
	default:
		return -1
	}
}

// customSpan is simply a wrapper around trace.Span to provide some commodity
// functions for adding attributes to the trace.
type customSpan struct {
	trace.Span
}

func newSpan(ctx context.Context, driver, query string, tracer trace.Tracer) (context.Context, *customSpan) {
	op := parseQueryOperation(query)

	ctx, span := tracer.Start(ctx, fmt.Sprintf("%s.%s", driver, op))
	defer span.End()

	span.SetAttributes(
		attribute.String("db.system", driver),
		attribute.String("db.operation", op),
		attribute.String("db.statement", query),
	)

	return ctx, &customSpan{span}
}

func (s *customSpan) addQueryParams(args ...interface{}) {
	for i := range args {
		v := fmt.Sprint(args[i])
		s.addQueryParams(i, v)
	}
}

func (s *customSpan) addAffectedRowsAttribute(n int64) {
	s.SetAttributes(attribute.Int64("db.result.returned_rows", n))
}

func (s *customSpan) addQueryParamAttribute(k, v string) {
	s.SetAttributes(attribute.String(fmt.Sprintf("db.statement.param_%s", k), v))
}
