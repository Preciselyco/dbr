package dialect

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgreSQL(t *testing.T) {
	for _, test := range []struct {
		in   string
		want string
	}{
		{
			in:   "table.col",
			want: `"table"."col"`,
		},
		{
			in:   "col",
			want: `"col"`,
		},
	} {
		require.Equal(t, test.want, PostgreSQL.QuoteIdent(test.in))
	}
}
