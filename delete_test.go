package dbr

import (
	"testing"

	"github.com/gocraft/dbr/v2/dialect"
	"github.com/stretchr/testify/require"
)

func TestDeleteStmt(t *testing.T) {
	buf := NewBuffer()
	builder := DeleteFrom("table").Where(Eq("a", 1)).Comment("DELETE TEST")
	err := builder.Build(dialect.MySQL, buf)
	require.NoError(t, err)
	require.Equal(t, "/* DELETE TEST */\nDELETE FROM `table` WHERE (`a` = ?)", buf.String())
	require.Equal(t, []interface{}{1}, buf.Value())
}

func TestDeleteReturning(t *testing.T) {
	sess := postgresSession
	reset(t, sess)

	_, err := sess.InsertInto("dbr_people").Columns("name", "email").Values(testName, testEmail).Exec()
	require.NoError(t, err)
	var person dbrPerson
	err = sess.DeleteFrom("dbr_people").Where(Eq("name", testName)).Returning("email").LoadOne(&person)
	require.NoError(t, err)
	require.Equal(t, person.Email, testEmail)
	require.Equal(t, person.Name, "")
	require.Equal(t, person.Id, int64(0))
	require.Len(t, sess.EventReceiver.(*testTraceReceiver).started, 2)
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].eventName, "dbr.select")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "DELETE")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "dbr_people")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "RETURNING")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "email")
	require.Equal(t, 2, sess.EventReceiver.(*testTraceReceiver).finished)
	require.Equal(t, 0, sess.EventReceiver.(*testTraceReceiver).errored)
}

func TestDeleteReturningAll(t *testing.T) {
	sess := postgresSession
	reset(t, sess)

	_, err := sess.InsertInto("dbr_people").Columns("name", "email").Values(testName, testEmail).Exec()
	require.NoError(t, err)
	var person dbrPerson
	err = sess.DeleteFrom("dbr_people").Where(Eq("name", testName)).Returning("*").LoadOne(&person)
	require.NoError(t, err)
	require.Equal(t, person.Email, testEmail)
	require.Equal(t, person.Name, testName)
	require.Equal(t, person.Id, int64(1))
	require.Len(t, sess.EventReceiver.(*testTraceReceiver).started, 2)
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "DELETE")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "dbr_people")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "RETURNING")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "id")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "name")
	require.Contains(t, sess.EventReceiver.(*testTraceReceiver).started[1].query, "email")
	require.Equal(t, 2, sess.EventReceiver.(*testTraceReceiver).finished)
	require.Equal(t, 0, sess.EventReceiver.(*testTraceReceiver).errored)
}

func BenchmarkDeleteSQL(b *testing.B) {
	buf := NewBuffer()
	for i := 0; i < b.N; i++ {
		DeleteFrom("table").Where(Eq("a", 1)).Build(dialect.MySQL, buf)
	}
}
