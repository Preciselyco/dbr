package dbr

import (
	"testing"

	"github.com/gocraft/dbr/v2/dialect"
	"github.com/stretchr/testify/require"
)

func TestUpdateStmt(t *testing.T) {
	buf := NewBuffer()
	builder := Update("table").Set("a", 1).Where(Eq("b", 2)).Comment("UPDATE TEST")
	err := builder.Build(dialect.PostgreSQL, buf)
	require.NoError(t, err)

	require.Equal(t, "/* UPDATE TEST */\nUPDATE \"table\" SET \"a\" = ? WHERE (\"b\" = ?)", buf.String())
	require.Equal(t, []interface{}{1, 2}, buf.Value())
}

func BenchmarkUpdateValuesSQL(b *testing.B) {
	buf := NewBuffer()
	for i := 0; i < b.N; i++ {
		Update("table").Set("a", 1).Build(dialect.PostgreSQL, buf)
	}
}

func BenchmarkUpdateMapSQL(b *testing.B) {
	buf := NewBuffer()
	for i := 0; i < b.N; i++ {
		Update("table").SetMap(map[string]interface{}{"a": 1, "b": 2}).Build(dialect.PostgreSQL, buf)
	}
}

func TestPostgresUpdateReturning(t *testing.T) {
	sess := postgresSession
	reset(t, sess)

	var ids []int
	err := sess.Update("dbr_people").Set("name", "Kordian").
		Where(Eq("id", 1)).Returning("id").Load(&ids)
	require.NoError(t, err)
	require.Len(t, sess.EventReceiver.(*testTraceReceiver).started, 1)
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[0].eventName, "dbr.select")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[0].query, "UPDATE")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[0].query, "dbr_people")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[0].query, "name")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[0].query, "RETURNING")
	require.Equal(t, 1, sess.EventReceiver.(*testTraceReceiver).finished)
	require.Equal(t, 0, sess.EventReceiver.(*testTraceReceiver).errored)
}

func TestPostgresUpdateReturningAll(t *testing.T) {
	sess := postgresSession
	reset(t, sess)
	_, err := sess.InsertInto("dbr_people").Columns("name", "email").Values(testName, testEmail).Exec()
	require.NoError(t, err)
	var persons []*dbrPerson
	newName := "Kordian"
	err = sess.Update("dbr_people").Set("name", newName).
		Where(Eq("id", 1)).Returning("*").Load(&persons)
	require.NoError(t, err)
	require.Len(t, persons, 1)
	require.Equal(t, persons[0].Email, testEmail)
	require.Equal(t, persons[0].Name, newName)
	require.Equal(t, persons[0].Id, int64(1))
	require.Len(t, sess.EventReceiver.(*testTraceReceiver).started, 2)
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].eventName, "dbr.select")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "UPDATE")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "dbr_people")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "name")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "RETURNING")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "id")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "name")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "email")
	require.Equal(t, 2, sess.EventReceiver.(*testTraceReceiver).finished)
	require.Equal(t, 0, sess.EventReceiver.(*testTraceReceiver).errored)

}
