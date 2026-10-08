package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
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

func Test_badgerBulk_LargeValues(t *testing.T) {
	dir, db := createTmpDB(BadgerImpl)
	defer func() {
		db.Close()
		os.RemoveAll(dir)
	}()

	bulk := db.NewBulk()

	// a value bigger than the batch size badger enforces on a single
	// transaction: the write batch must commit internally and keep writing
	bigValue := make([]byte, 12<<20)
	for i := range bigValue {
		bigValue[i] = byte(i)
	}
	for i := 0; i < 5; i++ {
		bulk.Set([]byte(fmt.Sprintf("big%d", i)), bigValue)
	}
	smallValue := make([]byte, 1<<20)
	for i := 0; i < 10; i++ {
		bulk.Set([]byte(fmt.Sprintf("small%d", i)), smallValue)
	}
	bulk.Flush()

	for i := 0; i < 5; i++ {
		assert.Equal(t, string(bigValue), string(db.Get([]byte(fmt.Sprintf("big%d", i)))), "big value %d", i)
	}
	for i := 0; i < 10; i++ {
		assert.Equal(t, string(smallValue), string(db.Get([]byte(fmt.Sprintf("small%d", i)))), "small value %d", i)
	}
}

// peakRSS returns the process high-water mark resident memory from
// /proc/self/status (Linux only; 0 when unavailable)
func peakRSS() uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			fields := strings.Fields(line)
			kb, _ := strconv.ParseUint(fields[1], 10, 64)
			return kb * 1024
		}
	}
	return 0
}

// Test_badgerBulk_MemoryBound writes far more data through a single bulk
// session than the transaction batch limits hold: the write batch must
// commit internally, so the live Go heap stays well below the written
// volume. Skipped in short mode
func Test_badgerBulk_MemoryBound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory bound test in short mode")
	}
	// shrink the block cache (Go heap) so heap usage reflects pending write
	// state, not cache capacity
	t.Setenv("BADGERDB_BLOCK_CACHE_SIZE_MB", "32")

	dir, dbi := createTmpDB(BadgerImpl)
	defer func() {
		dbi.Close()
		os.RemoveAll(dir)
	}()
	bdb := dbi.(*badgerDB)
	t.Logf("MaxBatchCount=%d MaxBatchSize=%d", bdb.db.MaxBatchCount(), bdb.db.MaxBatchSize())

	const entries = 1000000
	const valueSize = 1024
	// constant pattern for the value tail, index in the first 8 bytes
	valuePattern := make([]byte, valueSize)
	for i := range valuePattern {
		valuePattern[i] = byte(i)
	}

	var maxHeap uint64
	var memStats runtime.MemStats

	// the write batch retains references to the key and value slices until
	// its internal commits serialize them, so every entry gets its own fresh
	// buffers here (a reused buffer would be corrupted mid-commit)
	bulk := dbi.NewBulk()
	start := time.Now()
	for i := 0; i < entries; i++ {
		value := make([]byte, valueSize)
		copy(value, valuePattern)
		binary.BigEndian.PutUint64(value[:8], uint64(i))
		bulk.Set([]byte(fmt.Sprintf("key-%09d", i)), value)
		if i%10000 == 0 {
			runtime.ReadMemStats(&memStats)
			if memStats.HeapAlloc > maxHeap {
				maxHeap = memStats.HeapAlloc
			}
		}
	}
	bulk.Flush()
	elapsed := time.Since(start)

	runtime.ReadMemStats(&memStats)
	if memStats.HeapAlloc > maxHeap {
		maxHeap = memStats.HeapAlloc
	}
	rss := peakRSS()
	total := uint64(entries) * (14 + valueSize)
	t.Logf("wrote %d entries (%d MB) in %v, max live heap %d MB, peak RSS %d MB (incl mmap/page cache)",
		entries, total>>20, elapsed, maxHeap>>20, rss>>20)

	if maxHeap > total/2 {
		t.Fatalf("max live heap %d MB exceeded half the written volume (%d MB): the bulk is not self-batching", maxHeap>>20, total>>20)
	}

	// verify a sample of entries survived the internal commits
	valueCheck := make([]byte, valueSize)
	for i := 0; i < entries; i += 100 {
		copy(valueCheck, valuePattern)
		binary.BigEndian.PutUint64(valueCheck[:8], uint64(i))
		got := dbi.Get([]byte(fmt.Sprintf("key-%09d", i)))
		if !bytes.Equal(got, valueCheck) {
			t.Fatalf("entry %d mismatch: got[0:16]=%x want[0:16]=%x len=%d", i, got[:16], valueCheck[:16], len(got))
		}
	}
}
