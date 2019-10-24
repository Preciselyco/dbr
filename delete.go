package pqdbr

import (
	"context"
	"database/sql"
	"strconv"
)

// DeleteStmt builds `DELETE ...`.
type DeleteStmt struct {
	runner
	EventReceiver
	Dialect

	raw

	Table        string
	WhereCond    []Builder
	LimitCount   int64
	ReturnColumn []string

	comments Comments
}

type DeleteBuilder = DeleteStmt

func (b *DeleteStmt) Build(d Dialect, buf Buffer) error {
	if b.raw.Query != "" {
		return b.raw.Build(d, buf)
	}

	if b.Table == "" {
		return ErrTableNotSpecified
	}

	err := b.comments.Build(d, buf)
	if err != nil {
		return err
	}

	buf.WriteString("DELETE FROM ")
	buf.WriteString(d.QuoteIdent(b.Table))

	if len(b.WhereCond) > 0 {
		buf.WriteString(" WHERE ")
		err := And(b.WhereCond...).Build(d, buf)
		if err != nil {
			return err
		}
	}
	if b.LimitCount >= 0 {
		buf.WriteString(" LIMIT ")
		buf.WriteString(strconv.FormatInt(b.LimitCount, 10))
	}

	if len(b.ReturnColumn) > 0 {
		buf.WriteString(" RETURNING ")
		for i, col := range b.ReturnColumn {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(d.QuoteIdent(col))
		}
	}
	return nil
}

// DeleteFrom creates a DeleteStmt.
func DeleteFrom(table string) *DeleteStmt {
	return &DeleteStmt{
		Table:      table,
		LimitCount: -1,
	}
}

// DeleteFrom creates a DeleteStmt.
func (sess *Session) DeleteFrom(table string) *DeleteStmt {
	b := DeleteFrom(table)
	b.runner = sess
	b.EventReceiver = sess.EventReceiver
	b.Dialect = sess.Dialect
	return b
}

// DeleteFrom creates a DeleteStmt.
func (tx *Tx) DeleteFrom(table string) *DeleteStmt {
	b := DeleteFrom(table)
	b.runner = tx
	b.EventReceiver = tx.EventReceiver
	b.Dialect = tx.Dialect
	return b
}

// DeleteBySql creates a DeleteStmt from raw query.
func DeleteBySql(query string, value ...interface{}) *DeleteStmt {
	return &DeleteStmt{
		raw: raw{
			Query: query,
			Value: value,
		},
		LimitCount: -1,
	}
}

// DeleteBySql creates a DeleteStmt from raw query.
func (sess *Session) DeleteBySql(query string, value ...interface{}) *DeleteStmt {
	b := DeleteBySql(query, value...)
	b.runner = sess
	b.EventReceiver = sess.EventReceiver
	b.Dialect = sess.Dialect
	return b
}

// DeleteBySql creates a DeleteStmt from raw query.
func (tx *Tx) DeleteBySql(query string, value ...interface{}) *DeleteStmt {
	b := DeleteBySql(query, value...)
	b.runner = tx
	b.EventReceiver = tx.EventReceiver
	b.Dialect = tx.Dialect
	return b
}

// Where adds a where condition.
// query can be Builder or string. value is used only if query type is string.
func (b *DeleteStmt) Where(query interface{}, value ...interface{}) *DeleteStmt {
	switch query := query.(type) {
	case string:
		b.WhereCond = append(b.WhereCond, Expr(query, value...))
	case Builder:
		b.WhereCond = append(b.WhereCond, query)
	}
	return b
}

func (b *DeleteStmt) Limit(n uint64) *DeleteStmt {
	b.LimitCount = int64(n)
	return b
}

func (b *DeleteStmt) Comment(comment string) *DeleteStmt {
	b.comments = b.comments.Append(comment)
	return b
}

func (b *DeleteStmt) Returning(column ...string) *DeleteStmt {
	b.ReturnColumn = column
	return b
}

func (b *DeleteStmt) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

func (b *DeleteStmt) ExecContext(ctx context.Context) (sql.Result, error) {
	return exec(ctx, b.runner, b.EventReceiver, b, b.Dialect)
}

func (b *DeleteStmt) LoadContext(ctx context.Context, value interface{}) error {
	b.ReturnColumn = expandReturningAll(b.ReturnColumn, value)
	_, err := query(ctx, b.runner, b.EventReceiver, b, b.Dialect, value)
	return err
}

func (b *DeleteStmt) Load(value interface{}) error {
	return b.LoadContext(context.Background(), value)
}

func (b *DeleteStmt) LoadOneContext(ctx context.Context, value interface{}) error {
	b.ReturnColumn = expandReturningAll(b.ReturnColumn, value)
	count, err := query(ctx, b.runner, b.EventReceiver, b, b.Dialect, value)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func (b *DeleteStmt) LoadOne(value interface{}) error {
	return b.LoadOneContext(context.Background(), value)
}
