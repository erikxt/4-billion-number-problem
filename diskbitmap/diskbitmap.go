package main

import (
	"common"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

func parsePhoneNumberFromTempFile(file *os.File, startIndex uint64, endIndex uint64, dataChan chan uint64) {
	// convert uint64 to int64
	// read 1MB from the file
	size := uint64(1 << 20)
	for i := startIndex; i <= endIndex; i += size {
		buf := make([]byte, size)
		if endIndex-i < size {
			buf = make([]byte, endIndex-i+1)
		}
		file.ReadAt(buf, int64(i))
		// get the byte
		for index := range buf {
			b := buf[index]
			// check if the b is 0
			if b == 0 {
				continue
			}
			for j := 0; j < 8; j++ {
				// check if the bit is 1
				if b&(1<<uint8(j)) > 0 {
					n := (i+uint64(index))*8 + uint64(j)
					dataChan <- uint64(n)
				}
			}
		}
	}
	// close the channel
	close(dataChan)
}

// parse the phone number and write to the binary file
func parsePhoneNummberAndWriteToBin(data chan uint64, file *os.File) (maxIndex uint64, minIndex uint64) {
	c := 0
	maxIndex = uint64(0)
	minIndex = uint64(18446744073709551615)
	for num := range data {
		// calculate the index of the num in binary file
		index := num / 8
		// calculate the offset of the num in binary file
		offset := num % 8
		// set the bit to 1
		t := uint8(1 << offset)
		// read the byte from the file
		b := make([]byte, 1)
		_, err := file.ReadAt(b, int64(index))
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
		}
		// set the bit to 1
		// check if the byte array is not empty
		if len(b) == 0 {
			b = make([]byte, 1)
		}
		b[0] = b[0] | t

		// write the byte to the bufWriter
		_, err = file.WriteAt(b, int64(index))
		if err != nil {
			panic(err)
		}
		if index > maxIndex {
			maxIndex = index
		}
		if index < minIndex {
			minIndex = index
		}
		c++
	}
	fmt.Printf("total receive: %d\n", c)
	return maxIndex, minIndex
}

func main() {
	// record the start time
	start := time.Now()
	var inputPath string
	flag.StringVar(&inputPath, "input", "../duplicate.csv", "input file path")
	flag.Parse()
	// open the file
	file, err := os.Open(inputPath)
	if err != nil {
		fmt.Println(err)
		return
	}
	// close the file
	defer file.Close()
	// using goroutines to generate 4 billion phone numbers and write to a data channel
	data := make(chan uint64, 512) // data channel
	go common.ReadCsvFileAndWriteToChannel(data, file)
	// read the uint64 from the channel and write to a binary file which acts as a bitmap
	bitmap, err := os.Create("duplicate.bin")
	if err != nil {
		fmt.Println(err)
		return
	}
	// close the file
	defer os.Remove("duplicate.bin")
	defer bitmap.Close()
	// write the string to the file
	// minIndex, maxIndex helps to record the start and end index of the binary file
	// calculate the time cost of parsePhoneNummberAndWriteToBin
	startTime := time.Now()
	maxIndex, minIndex := parsePhoneNummberAndWriteToBin(data, bitmap)
	endTime := time.Now()
	fmt.Printf("parsePhoneNummberAndWriteToBin time: %v\n", endTime.Sub(startTime))

	fmt.Printf("max index: %d\n", maxIndex)
	// print minIndex
	fmt.Printf("min index: %d\n", minIndex)

	finalChan := make(chan uint64, 512)

	// use goroutine to parse the binary file, read 1MB from the file each time
	go parsePhoneNumberFromTempFile(bitmap, minIndex, maxIndex, finalChan)

	// create a file and write the string to the file
	resultFile, err := os.Create("diskbitmap_result.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	// close the file
	defer resultFile.Close()
	// write the string to the file
	common.ReadChanAndWriteToFile(resultFile, finalChan, "\n")
	// record the end time
	end := time.Now()
	// print the total time
	fmt.Printf("total time: %v\n", end.Sub(start))
}
