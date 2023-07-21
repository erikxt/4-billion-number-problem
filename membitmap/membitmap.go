package main

import (
	"bufio"
	"common"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

func ParsePhoneNumberFromBitmap(bitmap []byte, startIndex uint64, dataChan chan uint64) {
	// convert uint64 to int64
	for i := 0; i < len(bitmap); i++ {
		// get the byte
		b := bitmap[i]
		// check if the b is 0
		if b == 0 {
			continue
		}
		for j := 0; j < 8; j++ {
			// check if the bit is 1
			if b&(1<<uint8(j)) > 0 {
				n := startIndex + uint64(i*8+j)
				dataChan <- uint64(n)
			}
		}
		bitmap[i] = 0
	}
	close(dataChan)
}

// parse the phone number and write to the bitmap
func ParsePhoneNummberAndWriteToBitmap(data chan uint64, bitmap []byte, start uint64, size uint64) {
	c := 0
	for num := range data {
		if num < start || num > start+size-1 {
			continue
		}
		num = num - start
		// calculate the index of the num in bitmap
		index := num / 8
		// calculate the offset of the num in bitmap
		offset := num % 8
		// set the bit to 1
		t := uint8(1 << offset)
		// read the byte from the file
		b := bitmap[index]
		// set the bit to 1
		bitmap[index] = b | t
		c++
	}
	fmt.Printf("total receive: %d\n", c)
}

func useBitmapToFilterDupAndSave(file *os.File, index int, size uint64, bitmap []byte, resultFile *os.File) {
	// file rewind
	file.Seek(0, 0)

	// create a bufReader
	bufReader := bufio.NewReader(file)
	// create a scanner
	scanner := bufio.NewScanner(bufReader)
	// read line from bufReader
	for scanner.Scan() {
		// convert the string to uint64
		phoneNumber := scanner.Text()
		// convert the string to uint64
		num := common.StringToUint64(phoneNumber)
		// check if the num is in the range
		if num < uint64(index)*size || num > uint64(index)*size+size-1 {
			continue
		}
		num = num - uint64(index)*size
		// calculate the index of the num in bitmap
		index := num / 8
		// calculate the offset of the num in bitmap
		offset := num % 8
		// set the bit to 1
		t := uint8(1 << offset)
		// read the byte from the file
		b := bitmap[index]
		// set the bit to 1
		bitmap[index] = b | t
	}

	// parse the bitmap
	startIndex := uint64(index) * size
	finalDataChan := make(chan uint64, 512)
	// parse the binary file
	go ParsePhoneNumberFromBitmap(bitmap, startIndex, finalDataChan)

	common.ReadChanAndWriteToFile(resultFile, finalDataChan, "\n")
	// // clear the bitmap array iteratively
	// for i := 0; i < len(bitmap); i++ {
	// 	bitmap[i] = 0
	// }
}

// pre-process the data and split the data into small files
func preProcessCsvDataFile(size uint64, indexChanMap map[int]chan uint64,
	indexFileMap map[int]*os.File, file *os.File) {
	// create a wait group
	wg := sync.WaitGroup{}

	// using goroutines to generate 4 billion phone numbers and write to a csvDataChan channel
	csvDataChan := make(chan uint64, 512) // data channel
	go common.ReadCsvFileAndWriteToChannel(csvDataChan, file)

	var dataChan chan uint64
	var bufWriter *bufio.Writer
	for num := range csvDataChan {
		index := int(num / size)
		dataChan = indexChanMap[index]
		// check if index exists in the map
		if _, ok := indexChanMap[index]; !ok {
			// create a channel
			dataChan = make(chan uint64, 512)
			// add the channel to the map
			indexChanMap[index] = dataChan
			// create a file
			file, err := os.Create(fmt.Sprintf("./temp/%d.tmp", index))
			if err != nil {
				fmt.Println(err)
				return
			}
			// add the file to the map
			indexFileMap[index] = file
			// create a bufWriter
			bufWriter = bufio.NewWriterSize(file, 4096)

			// use goroutine to write the data to the file
			wg.Add(1)
			go func(dataChan chan uint64, bufWriter *bufio.Writer) {
				for num := range dataChan {
					// convert the uint64 to string
					phoneNumberString := fmt.Sprintf("%d\n", num)
					// write the string to the bufWriter
					_, err := bufWriter.WriteString(phoneNumberString)
					if err != nil {
						panic(err)
					}
				}
				// flush the bufWriter
				err := bufWriter.Flush()
				if err != nil {
					panic(err)
				}
				wg.Done()
			}(dataChan, bufWriter)
		}
		// write the num to the channel
		dataChan <- num
	}
	// close the channel
	for _, dataChan := range indexChanMap {
		close(dataChan)
	}
	// wait for all the goroutines to finish
	wg.Wait()
}

func main() {
	// record the start time
	start := time.Now()
	var inputPath string
	var bitmapSize int
	// read the uint64 from the channel and write to a 256 byte array)
	flag.StringVar(&inputPath, "input", "../duplicate.csv", "input file path")
	flag.IntVar(&bitmapSize, "size", 22, "bitmap size, default 22 means 4M, 28 means 256M")
	flag.Parse()
	size := uint64(1 << (bitmapSize + 3))
	// open the file
	file, err := os.Open(inputPath)
	if err != nil {
		fmt.Println(err)
		return
	}
	// close the file
	defer file.Close()
	// create a temp directory in child directory
	os.Mkdir("./temp", os.ModePerm)
	defer os.RemoveAll("./temp")

	// create a map to store int8 index and uint64 channel
	indexChanMap := make(map[int]chan uint64)
	// create a map to store int8 index and file pointer
	indexFileMap := make(map[int]*os.File)

	// split the data into small files
	// record the function start time
	preProcessStart := time.Now()
	preProcessCsvDataFile(size, indexChanMap, indexFileMap, file)
	preProcessEnd := time.Now()
	// print the pre-process time
	fmt.Printf("pre-process time: %v\n", preProcessEnd.Sub(preProcessStart))
	// sort key of the map
	var keys []int
	for k := range indexFileMap {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// create a result file
	resultFile, err := os.Create("membitmap_result.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	// iterate the map and parse the binary file
	bitmap := make([]byte, size>>3)
	for _, index := range keys {
		// open the file
		file := indexFileMap[index]
		// use bitmap to filter duplicate phone numbers
		useBitmapToFilterDupAndSave(file, index, size, bitmap, resultFile)
		// close the file
		file.Close()
		// delete the file
		os.Remove(file.Name())
		// delete the file pointer
		delete(indexFileMap, index)
		// delete the channel
		delete(indexChanMap, index)
	}

	end := time.Now()
	// print the total time
	fmt.Printf("total time: %v\n", end.Sub(start))
}
