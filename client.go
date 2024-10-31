package spg

import (
	"context"
	"fmt"
	"github.com/budka-tech/configo"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"time"
)

type Storage struct {
	Conn *pgx.Conn
}

type Client interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
	//BeginTx(ctx context.Context, txOptions pgx.TxOptions)
	//BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) error
}

func NewClient(ctx context.Context, cfg *configo.Database) (pool *pgxpool.Pool, err error) {
	dsn := Dsn(cfg)

	err = try(func() error {
		ctx, cancel := context.WithTimeout(ctx, cfg.AttemptDelay)
		defer cancel()

		pool, err = pgxpool.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("Ошибка при подключении к базе данных")
		}

		return nil
	}, cfg.MaxAttempts, cfg.AttemptDelay)

	if err != nil {
		return nil, fmt.Errorf("Не удалось подключиться к базе данных после %v попыток\n", cfg.MaxAttempts)
	} else {
		log.Println("Успешное подключение к базе данных!")
	}

	return pool, nil
}

func Dsn(cfg *configo.Database) string {
	return fmt.Sprintf("%v://%v:%v@%v:%v/%v", cfg.Type, cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)
}

func try(fn func() error, attempts int, delay time.Duration) (err error) {
	for attempts > 0 {
		if err = fn(); err != nil {
			time.Sleep(delay)
			attempts--

			continue
		}

		return nil
	}

	return
}
