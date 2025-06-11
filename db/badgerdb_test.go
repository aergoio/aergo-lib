package db

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func Test_badgerDB_CompactionController(t *testing.T) {
	// tmpDir, err := ioutil.TempDir("", "badgerdb-test-*")
	// if err != nil {
	// 	t.Fatal(err)
	// }

	tmpDir := "/tmp/badgerdb-test-3355012276"

	fmt.Println(tmpDir)

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
			got := NewDB(BadgerImpl, dir, Opt{
				Name:  "compactionController",
				Value: true,
			})

			const count = 1000000000 // 수십만 건 insert
			batch := got.NewBulk()
			for i := 0; i < count; i++ {
				key := []byte(fmt.Sprintf("asdasdkl;asdlkasdkjlasdkjasdlkjasdlkjqwlkjdlkwqdjklasjdklasjdaslkjdlkasjdklasjdlkasdjklqwjdlkqwjdklasjdlkasjdlkasjdlkasjdlkasjdlkjkey-%d", i))
				val := []byte(fmt.Sprintf("value-%d", i))
				batch.Set(key, val)

				if i%100000 == 0 {
					batch.Flush()
					batch = got.NewBulk()
					fmt.Println("flushing", i)
				}
			}
			batch.Flush()

			fmt.Println(" complete")

			time.Sleep(180000 * time.Millisecond)

			resp, err := http.Get("http://localhost:17091/compaction")
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}

			fmt.Println(resp)

			defer got.Close()
		})
	}
}
