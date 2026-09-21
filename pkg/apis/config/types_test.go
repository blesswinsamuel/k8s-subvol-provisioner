package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseOwnership(t *testing.T) {
	tests := []struct {
		input    string
		expected *Ownership
		wantErr  bool
	}{
		{input: "", expected: nil, wantErr: false},
		{input: "  ", expected: nil, wantErr: false},
		{input: "1000:1000", expected: &Ownership{UID: 1000, GID: 1000}, wantErr: false},
		{input: "0:0", expected: &Ownership{UID: 0, GID: 0}, wantErr: false},
		{input: "65534:65534", expected: &Ownership{UID: 65534, GID: 65534}, wantErr: false},
		{input: "invalid", expected: nil, wantErr: true},
		{input: "1000:", expected: nil, wantErr: true},
		{input: "abc:1000", expected: nil, wantErr: true},
	}

	for _, tt := range tests {
		res, err := ParseOwnership(tt.input)
		if tt.wantErr {
			assert.Error(t, err)
		} else {
			require.NoError(t, err)
			assert.Equal(t, tt.expected, res)
		}
	}
}

func TestParseFileMode(t *testing.T) {
	tests := []struct {
		input    string
		expected os.FileMode
		wantErr  bool
	}{
		{input: "0755", expected: 0755, wantErr: false},
		{input: "750", expected: 0750, wantErr: false},
		{input: "0700", expected: 0700, wantErr: false},
		{input: "invalid", wantErr: true},
		{input: "999", wantErr: true},
	}

	for _, tt := range tests {
		res, err := ParseFileMode(tt.input)
		if tt.wantErr {
			assert.Error(t, err)
		} else {
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, tt.expected, *res)
		}
	}
}

func TestParseKeyValueLines(t *testing.T) {
	raw := `
# Some comment
compression=zstd
atime: off
canmount = on
# another comment
`
	res := ParseKeyValueLines(raw)
	assert.Equal(t, map[string]string{
		"compression": "zstd",
		"atime":       "off",
		"canmount":    "on",
	}, res)
}
