package bash

import (
	"context"
	"io"
	"io/ioutil"
	"os/exec"
	"reflect"
	"strings"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DROP = "DROP"

func init() {
	database.Register("bash", &Bash{})
}

type Bash struct {
	Url               string
	Instance          interface{}
	CurrentVersion    int
	MigrationSequence []string
	LastRunMigration  []byte // todo: make []string
	IsDirty           bool
	IsLocked          bool

	Config *Config

	pool *pgxpool.Pool
}

type Config struct {
}

func (b *Bash) Open(url string) (database.Driver, error) {
	// create postgres connection string
	i := strings.Index(url, ":")
	postgresURL := url[i+3:]

	// connect to postgres
	pool, err := pgxpool.New(context.Background(), postgresURL)

	if err != nil {
		return nil, err
	}

	conn, err := pool.Acquire(context.Background())

	if err != nil {
		return nil, err
	}

	defer conn.Release()

	// check version table exists
	query := `
		create table if not exists public.bash_migrations (
			version BIGINT primary key,
			dirty BOOL NOT NULL DEFAULT FALSE
		);

		insert into public.bash_migrations(version)
		select t.version from (select -1 as version) t
		where not exists (select 1 from public.bash_migrations);
	`

	_, err = conn.Exec(context.Background(), query)

	if err != nil {
		return nil, err
	}

	return &Bash{
		Url:               url,
		CurrentVersion:    -1,
		MigrationSequence: make([]string, 0),
		Config:            &Config{},
		pool:              pool,
	}, nil
}

func (b *Bash) WithInstance(instance interface{}, config *Config) (database.Driver, error) {
	return &Bash{
		Instance:          instance,
		CurrentVersion:    -1,
		MigrationSequence: make([]string, 0),
		Config:            config,
	}, nil
}

func (b *Bash) Close() error {
	return nil
}

func (b *Bash) Lock() error {
	if b.IsLocked {
		return database.ErrLocked
	}
	b.IsLocked = true
	return nil
}

func (b *Bash) Unlock() error {
	b.IsLocked = false
	return nil
}

func (b *Bash) Run(migration io.Reader) error {
	m, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	ms := string(m)
	ms = strings.TrimSpace(ms)

	cmd := exec.Command("bash", "-c", ms)
	_, err = cmd.Output()

	if err != nil {
		return (err)
	}

	b.LastRunMigration = m
	b.MigrationSequence = append(b.MigrationSequence, string(m[:]))

	return nil
}

func (b *Bash) SetVersion(version int, state bool) error {
	conn, err := b.pool.Acquire(context.Background())

	if err != nil {
		return err
	}

	defer conn.Release()

	query := "update public.bash_migrations set version = $1, dirty = $2 where 1=1"

	_, err = conn.Exec(context.Background(), query, version, state)

	if err != nil {
		return err
	}

	b.CurrentVersion = version
	b.IsDirty = state
	return nil
}

func (b *Bash) Version() (version int, dirty bool, err error) {
	conn, err := b.pool.Acquire(context.Background())

	if err != nil {
		return 0, false, err
	}

	defer conn.Release()

	query := "select version, dirty from public.bash_migrations where 1=1"

	row := conn.QueryRow(context.Background(), query)

	err = row.Scan(&version, &dirty)

	if err != nil {
		return 0, false, err
	}

	defer conn.Release()
	return version, dirty, nil
}

func (b *Bash) Drop() error {
	conn, err := b.pool.Acquire(context.Background())

	if err != nil {
		return err
	}

	defer conn.Release()

	query := "update public.bash_migrations set version = -1 where 1=1"

	_, err = conn.Exec(context.Background(), query)

	if err != nil {
		return err
	}

	b.CurrentVersion = -1
	b.LastRunMigration = nil
	b.MigrationSequence = append(b.MigrationSequence, DROP)

	return nil
}

func (b *Bash) EqualSequence(seq []string) bool {
	return reflect.DeepEqual(seq, b.MigrationSequence)
}
