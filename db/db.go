package db

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/rs/zerolog"
)

type DBConfig struct {
	DriverName string
	DSN        string
}

type DatabaseConnection struct {
	DB     *sqlx.DB
	DBLock sync.RWMutex
	DBConfig
}

func ConnectToDB(log *zerolog.Logger, dbConfig DBConfig) *DatabaseConnection {
	if !strings.Contains(dbConfig.DSN, "?") {
		// Add the default connection options if none are given
		switch dbConfig.DriverName {
		case "sqlite3":
			dbConfig.DSN += "?_busy_timeout=5000&cache=shared"
		case "mysql":
			dbConfig.DSN += "?parseTime=true"
		}
	}

	db, err := sqlx.Open(dbConfig.DriverName, dbConfig.DSN)
	if err != nil {
		log.Fatal().Err(err).Msg("Could not open database")
	}

	// if dbConfig.DriverName == "sqlite3" {
	// 	db.SetMaxOpenConns(1)
	// }

	// note that we don't do db.SetMaxOpenConns(1), as we don't want to limit
	// read concurrency unnecessarily. sqlite will handle write locking on its
	// own, even across multiple processes accessing the same database file.
	// https://www.sqlite.org/faq.html#q5

	// we also don't enable the write-ahead-log because it does not work over a
	// networked filesystem

	return &DatabaseConnection{
		DB:       db,
		DBConfig: dbConfig,
	}
}

// UpdateRow wraps db.Exec and ensures that exactly one row was affected
func UpdateRow(DBConn *DatabaseConnection, query string, args ...any) error {
	return UpdateRowContext(context.Background(), DBConn, query, args...)
}

func UpdateRowContext(ctx context.Context, DBConn *DatabaseConnection, query string, args ...any) (err error) {
	if DBConn.DBConfig.DriverName == "sqlite3" {
		if err := lockContext(ctx, &DBConn.DBLock, true); err != nil {
			return err
		}
		defer DBConn.DBLock.Unlock()
	}

	res, err := DBConn.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return
	}

	count, err := res.RowsAffected()
	if err != nil {
		return
	}
	if count != 1 {
		err = fmt.Errorf("Expected 1 affected row, got %d", count)
	}
	return
}

// Defer the query itself until Scan, so SQLite's read lock spans the actual
// operation rather than just creation of a lazy sql.Row.
type Row struct {
	conn  *DatabaseConnection
	ctx   context.Context
	query string
	args  []any
}

func QueryRow(conn *DatabaseConnection, query string, args ...any) *Row {
	return QueryRowContext(context.Background(), conn, query, args...)
}
func QueryRowContext(ctx context.Context, conn *DatabaseConnection, query string, args ...any) *Row {
	return &Row{conn: conn, ctx: ctx, query: query, args: args}
}
func (row *Row) Scan(dest ...any) error {
	if row.conn.DriverName == "sqlite3" {
		if err := lockContext(row.ctx, &row.conn.DBLock, false); err != nil {
			return err
		}
		defer row.conn.DBLock.RUnlock()
	}
	return row.conn.DB.QueryRowContext(row.ctx, row.query, row.args...).Scan(dest...)
}

func Select(conn *DatabaseConnection, dest any, query string, args ...any) error {
	return SelectContext(context.Background(), conn, dest, query, args...)
}
func SelectContext(ctx context.Context, conn *DatabaseConnection, dest any, query string, args ...any) error {
	if conn.DriverName == "sqlite3" {
		if err := lockContext(ctx, &conn.DBLock, false); err != nil {
			return err
		}
		defer conn.DBLock.RUnlock()
	}
	return conn.DB.SelectContext(ctx, dest, query, args...)
}

func lockContext(ctx context.Context, mu *sync.RWMutex, write bool) error {
	if ctx.Done() == nil {
		if write {
			mu.Lock()
		} else {
			mu.RLock()
		}
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		acquired := false
		if write {
			acquired = mu.TryLock()
		} else {
			acquired = mu.TryRLock()
		}
		if acquired {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
