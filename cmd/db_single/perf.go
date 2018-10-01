package main

import (
	"bytes"
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

var valueLen = 256
var testSetSize = int64(10000000)
var batchSize = 1000
var dbDirPath = "e:/tmp"

var writeIter = 20000  //200000
var readIter = 1000000 //1000000

// parameters for drawing graph
var samplingRate = 1
var progressRate = 1000
var graphWidth = 120
var graphHeigh = 20

var timeFormatString = "Jan _2 15:04:05"

func init() {
	rand.Seed(time.Now().Unix())
}

func int64ToBytes(i testKeyType) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

var previousPrintSize = 0

func printProgress(max int, current int) {
	if max != 0 && max >= current {
		percentage := current * 100 / max
		printingText := fmt.Sprintf(" - %d / %d (%d%%)", current, max, percentage)
		fmt.Printf("\r%s\r%s", strings.Repeat(" ", previousPrintSize), printingText) //clear buffer and print again
		previousPrintSize = len(printingText)
	}
}

func printResult(statics list.List) {
	if statics.Len() == 0 {
		return
	}
	// convert list to slice
	var staticsSlice = make([]float64, statics.Len())
	i := 0
	//allZero := true
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

	fmt.Println("") // print empty line

	if average != 0.0 {
		// if all value is zero, asciigraph causes panic
		graph := asciigraph.Plot(staticsSlice, asciigraph.Width(graphWidth), asciigraph.Height(graphHeigh))
		fmt.Println(graph)
		fmt.Printf("* Time per op: %f ms\n", average*1000)
	} else {
		fmt.Println("all value is zero")
	}
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

func main() {

	argsWithoutProg := os.Args[1:]

	if len(argsWithoutProg) == 0 {
		fmt.Println("Usage: go run db_perf.go {dbImplTypeName}")
		os.Exit(0)
	}

	// generate a common random data set
	internal := map[testKeyType][]byte{}
	for i := 0; i < int(testSetSize); i++ {
		// generate a random value
		token := make([]byte, valueLen)
		internal[testKeyType(i)] = token
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

	var numOfWrite int64
	var idx testKeyType
	var statics list.List
	var startTime time.Time

	totalStartTime := time.Now()
	dbName := dbInstance.Type()

	fmt.Printf("[%s]\ntestset_size: %d\nkey_len: %d\nval_len: %d\n",
		dbName, testSetSize, reflect.TypeOf(idx).Size(), valueLen)
	fmt.Println("device type: ssd")
	fmt.Printf("batch size: %d\n", batchSize)

	// write only
	fmt.Println("[" + dbName + "-batch-write]")
	var i int
	for i = 0; i < writeIter; i++ {
		if i != 0 && i%samplingRate == 0 {
			startTime = time.Now()
		}
		tx := dbInstance.NewTx()

		for j := 0; j < batchSize; j++ {
			idx = testKeyType((int64(rand.Int()) % testSetSize)) // pick a random key
			rand.Read(internal[idx])                             // generate a random data

			tx.Set(
				int64ToBytes(testKeyType(idx)),
				internal[idx],
			)
		}
		tx.Commit()
		if i != 0 && i%samplingRate == 0 {
			endTime := time.Now().Sub(startTime).Seconds()
			statics.PushBack(endTime)
		}
		if i%progressRate == 0 {
			printProgress(writeIter, i)
		}
	}
	numOfWrite += int64(i)
	totalEndTime := time.Now()
	printResult(statics)
	statics.Init()

	fmt.Printf("* Total: %d iter during %v (%v ~ %v)\n", writeIter,
		totalEndTime.Sub(totalStartTime),
		totalStartTime.Format(timeFormatString),
		totalEndTime.Format(timeFormatString))

	totalStartTime = time.Now()

	// read only
	fmt.Println("[" + dbName + "-read]")
	for i := 0; i < readIter; i++ {
		if i != 0 && i%samplingRate == 0 {
			startTime = time.Now()
		}

		idx = testKeyType((int64(rand.Int()) % testSetSize))
		originalVal := internal[idx]

		retrievedVal := dbInstance.Get(int64ToBytes(testKeyType(idx)))

		if len(retrievedVal) != 0 {
			if len(retrievedVal) != valueLen {
				panic(fmt.Errorf("Expected length %X for %v, got %X",
					valueLen, idx, len(retrievedVal)))
			} else if !bytes.Equal(retrievedVal, originalVal) {
				panic(fmt.Errorf("Expected %v for %v, got %v",
					originalVal, idx, retrievedVal))
			}
		}
		if i != 0 && i%samplingRate == 0 {
			endTime := time.Now().Sub(startTime).Seconds()
			statics.PushBack(endTime)
		}
		if i%progressRate == 0 {
			printProgress(readIter, i)
		}
	}
	totalEndTime = time.Now()

	printResult(statics)
	statics.Init()
	fmt.Printf("* Total: %d iter during %v (%v ~ %v)\n", readIter,
		totalEndTime.Sub(totalStartTime),
		totalStartTime.Format(timeFormatString),
		totalEndTime.Format(timeFormatString))

	// close
	dbInstance.Close()

	size, err := dirSizeKB(tmpDir)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("* Final size of %s db: %v kb, Size of 1 write: %v byte\n", dbName, size, size*1024/numOfWrite)
	}
}
