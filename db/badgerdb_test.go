package db

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
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
	type env struct {
		Name, Value string
	}
	tests := []struct {
		name          string
		args          []env
		wantCompactor int
	}{
		{"default", []env{}, 4},
		{"zero", []env{{"BADGERDB_NUM_COMPACTORS", "0"}}, 0},
		{"withOthers", []env{
			{"BADGERDB_NO_COMPRESSION", "1"},
			{"BADGERDB_BLOCK_CACHE_SIZE_MB", "15"},
			{"BADGERDB_NUM_COMPACTORS", "3"},
		}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, envVar := range tt.args {
				err := os.Setenv(envVar.Name, envVar.Value)
				if err != nil {
					t.Fatal(err)
				}
			}
			defer func() {
				for _, envVar := range tt.args {
					_ = os.Unsetenv(envVar.Name)
				}
			}()

			dir := path.Join(tmpDir, tt.name)
			got := NewDB(BadgerImpl, dir)
			defer got.Close()

			actual := got.(*badgerDB)
			assert.Equalf(t, tt.wantCompactor, actual.db.Opts().NumCompactors, "badgerDB.EnvSet(%v)", tt.args)
		})
	}
}

var longTest = flag.Bool("long", false, "run long-running tests")

func Test_badgerDB_CompactionController(t *testing.T) {
	// tmpDir, err := ioutil.TempDir("", "badgerdb-test-*")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// 명시적으로 -long 이 있을 때만 실행함. eg) go test -v ./db/... -long
	if !*longTest {
		t.Skip("skipping long-running test; use -long flag to run")
	}

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
			got.SetCompactionEvent(func(event CompactionEvent) {
				if event.Start {
					fmt.Println("compaction at level", event.Level, "splits", event.NumSplits)
				} else {
					fmt.Println("compaction complete")
				}

			})

			const count = 1000000000 // 수십만 건 insert
			batch := got.NewBulk()
			for i := 0; i < count; i++ {
				key := []byte(fmt.Sprintf("asdasdkl;asdlkasdkjlasdkjasdlkjasdlkjqwlkjdlkwqdjklasjdklasjdaslkjdlkasjdklasjdlkasdjklqwjdlkqwjdklasjdlkasjdlkasjdlkasjdlkasjdlkjkey-%d", i))
				val := []byte(fmt.Sprintf("value-%d", i))
				batch.Set(key, val)

				if i%10000 == 0 {
					batch.Flush()
					batch = got.NewBulk()
				}
				if i%1000000 == 0 {
					fmt.Println("flushed", i)
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
