package db

import (
	"flag"
	"fmt"
	"github.com/aergoio/aergo-lib/log"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_newBadgerDB(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badgerdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	type args struct {
		opt []Option
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr assert.ErrorAssertionFunc
	}{
		{"default", args{[]Option{}}, badgerValueThreshold, assert.NoError},
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
	tmpDir, err := os.MkdirTemp("", "badgerdb-test-*")

	enableConfigure = true

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
		opt []Option
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr assert.ErrorAssertionFunc
	}{
		{"default", args{[]Option{}}, badgerValueThreshold, assert.NoError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := path.Join(tmpDir, tt.name)
			got := NewDB(BadgerImpl, dir, Option{
				Name:  "compactionController",
				Value: true,
			}, Option{
				Name: OptCompactionEventHandler,
				Value: func(event CompactionEvent) {
					if event.Start {
						fmt.Println("compaction at level", event.Level, "splits", event.NumSplits)
					} else {
						fmt.Println("compaction complete")
					}
				}})

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

func Test_readEnvInt64Value(t *testing.T) {
	if logger == nil {
		logger = newBadgerExtendedLog(log.NewLogger("db"))
	}

	type args struct {
		envName    string
		envValue   int64
		lowerLimit int64
		upperLimit int64
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"normal", args{"BADGER_MAX", 15, 4, 20}, false},
		{"tooLow", args{"BADGER_MAX", 1, 4, 20}, true},
		{"tooHigh", args{"BADGER_MAX", 21, 4, 20}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := os.Setenv(tt.args.envName, fmt.Sprintf("%d", tt.args.envValue))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = os.Unsetenv(tt.args.envName)
			}()
			var actual int64 = -1
			err = readEnvInt64Value(tt.args.envName, tt.args.lowerLimit, tt.args.upperLimit, &actual)
			if tt.wantErr != (err != nil) {
				t.Error("wantErr != (err != nil)")
			} else if !tt.wantErr {
				assert.Equal(t, tt.args.envValue, actual, fmt.Sprintf("readEnvInt64Value(%v, %v, %v, %v)", tt.args.envName, tt.args.lowerLimit, tt.args.upperLimit, actual))
			}
		})
	}
}

func Test_readEnvInt64ValueShift(t *testing.T) {
	if logger == nil {
		logger = newBadgerExtendedLog(log.NewLogger("db"))
	}
	// 0b10010
	os.Setenv("BADGER_MAX", fmt.Sprintf("%d", 18))
	defer func() {
		_ = os.Unsetenv("BADGER_MAX")
	}()
	tests := []struct {
		name  string
		shift int
		want  int64
	}{
		{"noshift", 0, 18},
		{"positive", 1, 36},
		{"negative", -1, 9},
		{"negative2", -2, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var actual int64 = -1
			err := readEnvInt64ValueShift("BADGER_MAX", 1, 20, &actual, tt.shift)
			if err != nil {
				t.Errorf("want no error but got error: %v", err)
			}
			assert.Equal(t, tt.want, actual)

		})
	}
}
