package common

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
)

func splitComma(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, ','); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, data[0:i], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

// read the csv file in parent directory by bufio and write to a channel
func ReadCsvFileAndWriteToChannel(data chan uint64, file *os.File) {
	// create a bufReader
	bufReader := bufio.NewReader(file)
	// create a scanner
	scanner := bufio.NewScanner(bufReader)

	scanner.Split(splitComma)
	for scanner.Scan() {
		// convert the string to uint64
		phoneNumber := scanner.Text()
		data <- StringToUint64(phoneNumber)
	}
	// close the channel
	close(data)
}

// converts a string to uint64
func StringToUint64(s string) uint64 {
	num, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return num
}

// write the string to the file from the channel
func ReadChanAndWriteToFile(file *os.File, data chan uint64, splitter string) {
	bufWriter := bufio.NewWriterSize(file, 4096)
	// write the string to the file
	c := 0
	for num := range data {
		// convert the uint64 to string
		phoneNumberString := fmt.Sprintf("%d%s", num, splitter)
		c++
		// write the string to the bufWriter
		_, err := bufWriter.WriteString(phoneNumberString)
		if err != nil {
			panic(err)
		}
	}
	// flush the bufWriter
	bufWriter.Flush()
}
