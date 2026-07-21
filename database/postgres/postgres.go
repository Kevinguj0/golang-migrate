package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"github.com/lib/pq"
)

func init() {
	db := Postgres{}
	database.Register("postgres", &db)
	database.Register("postgresql", &db)
}

var (
	DefaultMigrationsTable = "schema_migrations"
)

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNoSchema       = fmt.Errorf("no schema")
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
)

type Config struct {
	MigrationsTable string
	DatabaseName    string
	SchemaName      string
}

type Postgres struct {
	// Connection and transaction
	conn *sql.Conn
	db   *sql.DB

	// Locked state
	isLocked bool

	// Config
	config *Config
}

func WithInstance(db *sql.DB, config *Config) (database.Driver, error) {
	if err := db.Ping(); err != nil {
		return nil, err
	}

	if config == nil {
		return nil, ErrNilConfig
	}

	if len(config.DatabaseName) == 0 {
		return nil, ErrNoDatabaseName
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	px := &Postgres{
		conn:   conn,
		db:     db,
		config: config,
	}

	if err := px.ensureVersionTable(); err != nil {
		return nil, err
	}

	return px, nil
}

func (p *Postgres) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", migrateDSN(purl))
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	schemaName := purl.Query().Get("search_path")
	if len(schemaName) == 0 {
		schemaName = "public"
	}

	px, err := WithInstance(db, &Config{
		DatabaseName:    purl.Path,
		MigrationsTable: migrationsTable,
		SchemaName:      schemaName,
	})
	if err != nil {
		return nil, err
	}

	return px, nil
}

func (p *Postgres) Close() error {
	var err error
	if p.conn != nil {
		err = p.conn.Close()
	}
	return err
}

// Lock implements database.Driver.Lock.
func (p *Postgres) Lock() error {
	if p.isLocked {
		return database.ErrLocked
	}

	query := `SELECT pg_advisory_lock($1)`
	if _, err := p.conn.ExecContext(context.Background(), query, p.lockIdentifier()); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	p.isLocked = true
	return nil
}

// Unlock implements database.Driver.Unlock.
func (p *Postgres) Unlock() error {
	if !p.isLocked {
		return nil
	}

	query := `SELECT pg_advisory_unlock($1)`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := p.conn.ExecContext(ctx, query, p.lockIdentifier()); err != nil {
		if p.conn != nil {
			p.conn.Close()
		}
		p.isLocked = false
		return nil
	}
	p.isLocked = false
	return nil
}

// Run implements database.Driver.Run.
func (p *Postgres) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	query := string(migr)
	if _, err := p.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

// SetVersion implements database.Driver.SetVersion.
func (p *Postgres) SetVersion(version int, dirty bool) error {
	tx, err := p.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte("begin tx")}
	}

	query := `TRUNCATE ` + p.config.MigrationsTable
	if _, err := tx.ExecContext(context.Background(), query); err != nil {
		tx.Rollback()
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if version >= 0 {
		query = `INSERT INTO ` + p.config.MigrationsTable + ` (version, dirty) VALUES ($1, $2)`
		if _, err := tx.ExecContext(context.Background(), query, version, dirty); err != nil {
			tx.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte("commit tx")}
	}

	return nil
}

// Version implements database.Driver.Version.
func (p *Postgres) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM ` + p.config.MigrationsTable + ` LIMIT 1`
	err = p.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil
	case err != nil:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	default:
		return version, dirty, nil
	}
}

// Drop implements database.Driver.Drop.
func (p *Postgres) Drop() error {
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema = $1`
	rows, err := p.conn.QueryContext(context.Background(), query, p.config.SchemaName)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer rows.Close()

	var rerr error
	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(tables) > 0 {
		query = `DROP TABLE IF EXISTS ` + strings.Join(tables, ",") + ` CASCADE`
		if _, err := p.conn.ExecContext(context.Background(), query); err != nil {
			rerr = multierror.Append(rerr, &database.Error{OrigErr: err, Query: []byte(query)})
		}
	}

	query = `DROP TABLE IF EXISTS ` + p.config.MigrationsTable
	if _, err := p.conn.ExecContext(context.Background(), query); err != nil {
		rerr = multierror.Append(rerr, &database.Error{OrigErr: err, Query: []byte(query)})
	}

	return rerr
}

func (p *Postgres) ensureVersionTable() error {
	var exists bool
	query := `SELECT EXISTS (
		SELECT 1
		FROM   information_schema.tables 
		WHERE  table_schema = $1
		AND    table_name = $2
	)`
	if err := p.conn.QueryRowContext(context.Background(), query, p.config.SchemaName, p.config.MigrationsTable).Scan(&exists); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	if exists {
		return nil
	}

	query = `CREATE TABLE ` + p.config.MigrationsTable + ` (version bigint not null primary key, dirty boolean not null)`
	if _, err := p.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (p *Postgres) lockIdentifier() int64 {
	return int64(database.GenerateAdvisoryLockId(p.config.DatabaseName))
}

func migrateDSN(url *url.URL) string {
	if url == nil {
		return ""
	}

	q := url.Query()
	q.Del("x-migrations-table")
	url.RawQuery = q.Encode()

	return url.String()
}
