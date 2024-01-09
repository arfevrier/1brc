package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"sync"
)

type CityName [64]byte
type CityValues struct {
	min int64
	max int64
	sum int64
	len int64
}

const numWorkers = 8
const chunkSize = 256 * 1024
const bufSize = 1024

func worker(chanLine chan []byte, chanTemp chan map[CityName]CityValues) {
	// Results map
	cityTemps := make(map[CityName]CityValues, 512)
	for line := range chanLine {
		// !!!
		// Still need to implement one more loop to read each chunk line
		// !!!
		sepI := bytes.Index(line, []byte{';'})

		//Read city name
		city := CityName{}
		copy(city[:], line[:sepI])

		//Read city value
		var sign int64 = 1
		if line[sepI+1] == '-' {
			sign = -1
			sepI++
		}
		//Move sep to the first number
		sepI++

		currentCitiesValue, ok := cityTemps[city]
		var calcValue int64
		if !ok {
			currentCitiesValue.min = math.MaxInt64
			currentCitiesValue.max = math.MinInt64
		}
		//If is a two char number
		if line[sepI+2] == '.' {
			calcValue = sign * (int64(line[sepI]-'0')*100 + int64(line[sepI+1]-'0')*10 + int64(line[sepI+3]-'0'))
			currentCitiesValue.sum += calcValue
			currentCitiesValue.len += 1
		} else {
			calcValue = sign * (int64(line[sepI]-'0')*10 + int64(line[sepI+2]-'0'))
			currentCitiesValue.sum += calcValue
			currentCitiesValue.len += 1
		}
		if currentCitiesValue.min > calcValue {
			currentCitiesValue.min = calcValue
		}
		if currentCitiesValue.max < calcValue {
			currentCitiesValue.max = calcValue
		}
		cityTemps[city] = currentCitiesValue
		// fmt.Printf("%s, %+v\n", city, cityTemps[city])
	}
	chanTemp <- cityTemps
}

func main() {
	// f, err := os.Create("trace.out")
	// if err != nil {
	// 	panic(err)
	// }
	// defer f.Close()

	// err = trace.Start(f)
	// if err != nil {
	// 	panic(err)
	// }
	// defer trace.Stop()

	if len(os.Args) < 2 {
		fmt.Println("Error: First arg should be the measurements.txt file path")
		return
	}

	filePath := os.Args[1]
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	// Create the file buffer
	buf := bufio.NewReaderSize(file, chunkSize) //force min chunk size to 256 * 1024

	// Transfert channel
	chanLine := make(chan []byte)

	// Result channel
	chanTemp := make(chan map[CityName]CityValues)

	// Waitgroup
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(chanLine, chanTemp)
		}()
	}

	final := map[CityName]CityValues{}
	go func() {
		for {
			// Read a chunk of 1024
			line, _ := buf.Peek(bufSize)
			if len(line) == 0 {
				break
			}
			// Split to the last \n
			sepLI := bytes.LastIndex(line, []byte{'\n'})
			// Only copy complete line
			copyLine := make([]byte, sepLI)
			copy(copyLine, line)
			// Send the chunk to the worker
			chanLine <- copyLine
			// Discard sended chunk
			buf.Discard(sepLI + 1)
		}
		// CLose the line feed
		close(chanLine)
		// Read and compile the worker result
		for result := range chanTemp {
			for k, v := range result {
				// Get cities from map
				currentCitiesValue, ok := final[k]
				// Set min,max if citie already exist
				if ok {
					currentCitiesValue.min = currentCitiesValue.min ^ ((v.min ^ currentCitiesValue.min) & ((v.min - currentCitiesValue.min) >> 63))
					currentCitiesValue.max = currentCitiesValue.max ^ ((currentCitiesValue.max ^ v.max) & ((currentCitiesValue.max - v.max) >> 63))
				} else {
					currentCitiesValue.min = v.min
					currentCitiesValue.max = v.max
				}
				// Add sum, len and min/max
				currentCitiesValue.sum += v.sum
				currentCitiesValue.len += v.len
				//Then, insert back the citie
				final[k] = currentCitiesValue
			}
		}
	}()
	wg.Wait()
	close(chanTemp)
	// Print the result
	for k, v := range final {
		fmt.Printf("{`%s`: %+v},", k, v)
	}
}
