package db

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"path"
	"testing"
)

func Test_newBadgerDB(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "badgerdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	type args struct {
		opt []Opt
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr assert.ErrorAssertionFunc
	}{
		{"default", args{[]Opt{}}, badgerValueThreshold, assert.NoError},
		{"default", args{[]Opt{Opt{OptBadgerValueThreshold, int64(3330)}}}, 3330, assert.NoError},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := path.Join(tmpDir, tt.name)
			got := NewDB(BadgerImpl, dir, tt.args.opt...)
			defer got.Close()
			if !tt.wantErr(t, err, fmt.Sprintf("newBadgerDB(%v, %v)", dir, tt.args.opt)) {
				return
			}
			actual := got.(*badgerDB)
			assert.Equalf(t, tt.want, actual.db.Opts().ValueThreshold, "newBadgerDB(%v, %v)", dir, tt.args.opt)
		})
	}
}

func Test_badgerDB_EnvSet(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "badgerdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	type args struct {
		opt []Opt
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr assert.ErrorAssertionFunc
	}{
		{"default", args{[]Opt{}}, badgerValueThreshold, assert.NoError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := path.Join(tmpDir, tt.name)
			got := NewDB(BadgerImpl, dir, tt.args.opt...)
			defer got.Close()
		})
	}
}
