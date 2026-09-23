package identity

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGeneratedIdentifiers(t *testing.T) {
	codes, ids := map[string]bool{}, map[string]bool{}
	for i := 0; i < 1000; i++ {
		c, err := Code()
		require.NoError(t, err)
		require.True(t, ValidCode(c))
		require.False(t, codes[c], "duplicate code")
		codes[c] = true
		id, err := UUID()
		require.NoError(t, err)
		require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, id)
		require.False(t, ids[id], "duplicate UUID")
		ids[id] = true
	}
}
func TestValidCode(t *testing.T) {
	for _, tc := range []struct {
		code  string
		valid bool
	}{{"0123456789", true}, {"aAzZ019876", true}, {"", false}, {"123456789", false}, {"12345678901", false}, {"123456789_", false}, {"123456789/", false}, {"é23456789", false}} {
		assert.Equal(t, tc.valid, ValidCode(tc.code), tc.code)
	}
}
