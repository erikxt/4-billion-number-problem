package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Generate random Chinese phone numbers with 11 digits and the number must start with number one and write to a channel
func CreateRandomPhoneNumberAndWriteToChannel(data chan uint64, wg *sync.WaitGroup, iterateTimes uint) {
	for i := uint(0); i < iterateTimes; i++ {
		// generate the 11 digits of the phone number starts with number one randomly in uint64
		phoneNumber := rand.Uint64()%10000000000 + 10000000000
		// write the string to the channel
		data <- phoneNumber
	}
	// signal done
	wg.Done()
}

// consume uint64 from channel and signal when done
func ConsumeStringFromChannel(data chan uint64, done chan bool) {
	// create a file and write the string to the file
	file, err := os.Create("../duplicate.csv")
	if err != nil {
		fmt.Println(err)
		return
	}
	// close the file
	defer file.Close()
	ReadChanAndWriteToFile(file, data)
	// signal done
	done <- true
}

func ReadChanAndWriteToFile(file *os.File, data chan uint64) {
	bufWriter := bufio.NewWriter(file)
	// write the string to the file
	c := 0
	for num := range data {
		// convert the uint64 to string
		phoneNumberString := fmt.Sprintf("%d,", num)
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

func Process(total uint, goroutineCount int) {
	// using goroutines to generate 4 billion phone numbers and write to a data channel
	data := make(chan uint64, 512) // data channel
	done := make(chan bool)        // done channel
	wg := sync.WaitGroup{}         // wait group
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		count := total / uint(goroutineCount)
		// fmt.Printf("goroutine %d: %d\n", i, count)
		go CreateRandomPhoneNumberAndWriteToChannel(data, &wg, count)
	}
	go ConsumeStringFromChannel(data, done)
	// go func() {
	wg.Wait()
	close(data)
	// }()
	<-done
}

func main() {
	// read time cost
	start := time.Now()
	// read the total count of phone numbers
	var total uint
	flag.UintVar(&total, "total", 40_0000_0000, "total count of phone numbers")
	flag.Parse()
	// print the total count of phone numbers
	fmt.Printf("total count of phone numbers: %d\n", total)
	goroutineCount := 10
	Process(total, goroutineCount)
	end := time.Now()
	// print the total time
	fmt.Printf("total time: %v\n", end.Sub(start))
}
