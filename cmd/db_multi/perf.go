package main

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/aergoio/aergo-lib/db"
	"github.com/guptarohit/asciigraph"
)

type testKeyType int64

var writerGoroutineNum = 5
var readerGoroutineNum = 30

//var gomaxprocsMultiplier = 4

var valueLen = 256
var testSetSize = int64(10000000)
var batchSize = 1000
var dbDirPath = "" //empty string = os default temp path

var writeIter = 100000 //200000

// parameters for drawing graph
var progressRate = 100
var graphWidth = 120
var graphHeigh = 15

var timeFormatString = "Jan _2 15:04:05"

var dummy testKeyType

// initialize channels
var quitReaderSignalChannel = make(chan struct{})
var notifyWriterFinishChan = make(chan struct{})
var reportwriterProgressChan = make(chan writerProgress, 1000)
var resultChan = make(chan string, writerGoroutineNum)

func init() {
	rand.Seed(time.Now().Unix())
}

func int64ToBytes(i testKeyType) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

var previousPrintSize = 0

func printWritersProgress(seqProgrMap map[int]int) {
	printingText := ""

	for writerSeq := 0; writerSeq < writerGoroutineNum; writerSeq++ {
		curProgress := seqProgrMap[writerSeq]
		percentage := curProgress * 100 / writeIter
		printingText += fmt.Sprintf(" [%d] %d / %d (%d%%)", writerSeq, curProgress, writeIter, percentage)
	}

	fmt.Printf("\r%s\r%s", strings.Repeat(" ", previousPrintSize), printingText) //clear buffer and print again
	previousPrintSize = len(printingText)
}

func clearProgress() {
	fmt.Printf("\r%s\r", strings.Repeat(" ", previousPrintSize))
}

// this returns (graph, time per op ms, number of item)
func genGraph(statics list.List) (string, float64, int) {
	if statics.Len() == 0 {
		return "", 0.0, 0
	}
	// convert list to slice
	var staticsSlice = make([]float64, statics.Len())
	i := 0

	var average float64
	for e := statics.Front(); e != nil; e = e.Next() {
		staticsSlice[i] = e.Value.(float64)
		average += e.Value.(float64)
		if e.Value.(float64) != 0.0 {
			//		allZero = false
		}
		i++
	}

	if i != 0 {
		average = average / float64(i)
	}

	if average != 0.0 {
		// if all value is zero, asciigraph causes panic
		graph := asciigraph.Plot(staticsSlice, asciigraph.Width(graphWidth), asciigraph.Height(graphHeigh))

		return graph, average * 1000, statics.Len()
	}

	return "", 0.0, statics.Len()
}

func dirSizeKB(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size / 1024, err
}

type writerProgress struct {
	seq      int
	progress int
}

func invokeWriter(dbInstance db.DB, seq int) {
	var statics list.List
	var startTime time.Time

	randomVal := make([]byte, valueLen)

	for i := 0; i < writeIter; i++ {
		startTime = time.Now()
		tx := dbInstance.NewTx()

		for j := 0; j < batchSize; j++ {
			idx := testKeyType((int64(rand.Int()) % testSetSize)) // pick a random key
			rand.Read(randomVal)                                  // generate a random data

			tx.Set(
				int64ToBytes(testKeyType(idx)),
				randomVal,
			)
		}
		tx.Commit()

		endTime := time.Now().Sub(startTime).Seconds()
		statics.PushBack(endTime)

		if i%progressRate == 0 {
			reportwriterProgressChan <- writerProgress{seq, i}
		}
	}

	graph, avrg, count := genGraph(statics)

	resultChan <- fmt.Sprintf("[Writer#%d] - write: %d, latency: %f ms\n%s", seq, count, avrg, graph)

	// let parent know
	notifyWriterFinishChan <- struct{}{}
}

func invokeReader(dbInstance db.DB, seq int) {
	var statics list.List
	var startTime time.Time
	var i uint64
EXITLOOP:
	for {
		select {
		case <-quitReaderSignalChannel:
			break EXITLOOP
		default:
			i++
			startTime = time.Now()

			idx := testKeyType((int64(rand.Int()) % testSetSize))

			dbInstance.Get(int64ToBytes(testKeyType(idx)))

			endTime := time.Now().Sub(startTime).Seconds()
			statics.PushBack(endTime)
		}
	}

	graph, avrg, count := genGraph(statics)

	resultChan <- fmt.Sprintf("[Reader#%d] - read: %d, latency: %f ms\n%s", seq, count, avrg, graph)
}

func main() {

	argsWithoutProg := os.Args[1:]

	if len(argsWithoutProg) == 0 {
		fmt.Println("Usage: go run db_perf.go {dbImplTypeName}")
		os.Exit(0)
	}

	tmpDir, _ := ioutil.TempDir(dbDirPath, argsWithoutProg[0])
	dbType := db.ImplType(argsWithoutProg[0])
	var dbInstance db.DB
	{
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Unknow db.ImplType: " + dbType)
				os.Exit(1)
			}
		}()
		dbInstance = db.NewDB(dbType, tmpDir)
	}
	defer os.Remove(tmpDir)

	totalStartTime := time.Now()
	dbName := dbInstance.Type()

	fmt.Printf("[%s]\ntestset_size: %d\nkey_len: %d\nval_len: %d\n",
		dbName, testSetSize, reflect.TypeOf(dummy).Size(), valueLen)
	fmt.Println("data path: " + tmpDir)
	fmt.Printf("batch size: %d\n", batchSize)

	// invoke reader
	for i := 0; i < readerGoroutineNum; i++ {
		go invokeReader(dbInstance, i)
	}

	// invoke writer
	for i := 0; i < writerGoroutineNum; i++ {
		go invokeWriter(dbInstance, i)
	}

	// waiting until all writers are finished
	writerDoneCnt := 0
	progressMap := make(map[int]int)
FINISHWORK:
	for {
		select {
		case <-notifyWriterFinishChan:
			writerDoneCnt++
		case progress := <-reportwriterProgressChan:
			progressMap[progress.seq] = progress.progress
		default:
			// all writers are finished
			if writerDoneCnt == writerGoroutineNum {

				break FINISHWORK
			} else {
				// print progress
				printWritersProgress(progressMap)
				time.Sleep(time.Second)
			}
		}
	}

	clearProgress()

	totalEndTime := time.Now()

	// send a quit signal to readers
	close(quitReaderSignalChannel)

	// collect and print all results from working gorotines
	getResultCnt := 0
	for text := range resultChan {
		fmt.Println("\n" + text)
		getResultCnt++
		if getResultCnt == writerGoroutineNum+readerGoroutineNum {
			break
		}
	}

	fmt.Printf("\n* Total: %d goroutine write %d tx, %d goroutine read, during %v (%v ~ %v)\n",
		writerGoroutineNum, writeIter, readerGoroutineNum,
		totalEndTime.Sub(totalStartTime),
		totalStartTime.Format(timeFormatString),
		totalEndTime.Format(timeFormatString))

	// close
	dbInstance.Close()

	// measure disk usage
	if writerGoroutineNum != 0 && writeIter != 0 {
		size, err := dirSizeKB(tmpDir)
		if err != nil {
			fmt.Println("fail to get directory size")
			fmt.Println(err)
		} else {
			fmt.Printf("* Final size of %s db: %v kb, Size of 1 write: %v byte\n", dbName, size, size*1024/int64(writerGoroutineNum*writeIter))
		}
	}
}
