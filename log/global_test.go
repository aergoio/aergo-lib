package log

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFindProjectRoot(t *testing.T) {
	t.Skip("This test depends on the developer's environment")
	tests := []struct {
		name string
		want string
	}{
		// TODO: Add test cases.
		{"base", "/home/hayarobipark/Developer/aergo/aergo-lib"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, FindProjectRoot(), "FindProjectRoot()")
		})
	}
}
