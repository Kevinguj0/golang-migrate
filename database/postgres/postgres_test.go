package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/lib/pq"
)

func Test(t *testing.T) {
	purl := os.Getenv("POSTGRES_PORT_5432_TCP")
	if purl == "" {
		t.Skip("postgres container not running")
	}

	db, err := sql.Open("postgres", purl)
	if err != nil {
		t.Fatal(err)
	}

	p := &Postgres{}
	d, err := p.Open(purl)
	if err != nil {
		t.Fatal(err)
	}

	dt.Test(t, d, []byte("SELECT 1"))
}

func TestAdvisoryLockContextCancellation(t *testing.T) {
	purl := os.Getenv("POSTGRES_PORT_5432_TCP")
	if purl == "" {
		t.Skip("postgres container not running")
	}

	db, err := sql.Open("postgres", purl)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	p := &Postgres{
		conn: conn,
		db:   db,
		config: &Config{
			DatabaseName: "postgres",
		},
	}

	if err := p.Lock(); err != nil {
		t.Fatal(err)
	}

	cancel()

	if err := p.Unlock(); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	conn2, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	p2 := &Postgres{
		conn: conn2,
		db:   db,
		config: &Config{
			DatabaseName: "postgres",
		},
	}

	if err := p2.Lock(); err != nil {
		t.Fatalf("failed to acquire lock on second connection: %v", err)
	}
	defer p2.Unlock()
}
