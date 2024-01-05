/*

Corlin Palmer's Go solution to the 1BRC coding challenge: https://github.com/gunnarmorling/1brc

- This solution reads the file sequentially as fast as possible (reader)
- It passes off the job of ensuring that each chunk ends with a complete line to another goroutine (lineSupervisor)
- The lineSupervisor sends valid chunks to a pool of worker goroutines (worker) which parse the data and calculate the results
- The results from the workers are collected in a map and then sorted before printing the final results

A fair amount of optimization has been done to reduce memory allocations.
Still, it's currently 2X faster than the best Java implementation (on my machine, a Macbook M3 Pro).

Script	                                Time (s)
calculate_average_corlinp.sh	           5.092
calculate_average_warpspeedlabs.sh         8.183  (Go)
calculate_average_ddimtirov.sh            10.183
calculate_average_ebarlas.sh	          11.625
calculate_average_royvanrijn.sh	          11.970
calculate_average_AlexanderYastrebov.sh	  14.114  (Go)
calculate_average_filiphr.sh	          15.038
calculate_average_palmr.sh	          15.359
calculate_average_spullara.sh	          16.350
calculate_average_seijikun.sh	          20.214
calculate_average_padreati.sh	          22.460
calculate_average_richardstartin.sh	  22.496
calculate_average_bjhara.sh	          23.166
calculate_average_criccomini.sh	          23.378
calculate_average_truelive.sh	          25.741
calculate_average_khmarbaise.sh	          41.079
calculate_average_kuduwa-keshavram.sh	  45.390
calculate_average_itaske.sh	          50.250
calculate_average.sh	                 162.420


For some fun comparisons, it's even faster than wc -l, but still slower than piping it to /dev/null:

cat measurements.txt > /dev/null           3.522
wc -l measurements.txt			  11.892

*/

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

// reader reads raw chunks from the file and sends them to the rawChunks channel.
func reader(file *os.File, rawChunks chan<- []byte) {
	const chunkSize = 256 * 1024
	buf := make([]byte, chunkSize)

	for {
		bytesRead, err := file.Read(buf)
		if bytesRead > 0 {
			chunk := make([]byte, bytesRead)
			copy(chunk, buf[:bytesRead])
			rawChunks <- chunk
		}
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Error reading file: %v\n", err)
			}
			break
		}
	}
	close(rawChunks)
}

// processChunk processes a single chunk and returns the valid part and the leftover part.
func processChunk(chunk, leftover []byte) (validPart, newLeftover []byte) {
	// Find the first and last newline to determine the valid part of the chunk
	firstNewline := bytes.Index(chunk, []byte{'\n'})
	lastNewline := bytes.LastIndex(chunk, []byte{'\n'})

	if firstNewline != -1 {
		// There's a complete line at the beginning of the chunk
		validPart = append(leftover, chunk[:firstNewline+1]...)
		leftover = leftover[:0] // Clear the leftover
	} else {
		// No complete line at the start, append the whole chunk to the leftover
		leftover = append(leftover, chunk...)
	}

	if lastNewline != -1 && firstNewline != lastNewline {
		// There's at least one complete line in this chunk
		// Include the middle part of the chunk, which contains only complete lines
		validPart = append(validPart, chunk[firstNewline+1:lastNewline+1]...)

		// Store the dangling end (if any) in newLeftover for the next chunk
		newLeftover = append(newLeftover, chunk[lastNewline+1:]...)
	} else {
		// No complete lines or only one complete line in this chunk
		newLeftover = leftover
	}

	return validPart, newLeftover
}

// lineSupervisor takes raw chunks, ensures they end with complete lines, and sends valid chunks to the workers.
func lineSupervisor(rawChunks <-chan []byte, validChunks chan<- []byte) {
	buffer := make([]byte, 0) // Buffer to hold the dangling ends

	for chunk := range rawChunks {
		validPart, newBuffer := processChunk(chunk, buffer)
		if len(validPart) > 0 {
			validChunks <- validPart
		}
		buffer = newBuffer
	}

	// Handle any data left in the buffer after all chunks have been processed
	if len(buffer) > 0 {
		validChunks <- buffer
	}

	close(validChunks)
}

type CityName [64]byte

// [0] = sum of all temperatures
// [1] = number of temperatures
// [2] = min temperature
// [3] = max temperature
type CityTemp *[4]int64

// worker parses the lines in a chunk and performs some calculations
func worker(validChunks <-chan []byte, results chan<- map[CityName]CityTemp) {
	cityTemps := make(map[CityName]CityTemp, 512)
	var city CityName
	cityLen := 0
	var temp int64
	var isNegative bool

	for chunk := range validChunks {
		for _, b := range chunk {
			switch b {
			case ';': // Delimiter between city and temperature
				cityLen = 64
				isNegative = false
			case '\n':
				if isNegative {
					temp = -temp
				}
				ct := cityTemps[city]
				if ct == nil {
					minMaxTemp := temp
					if isNegative {
						minMaxTemp = -minMaxTemp
					}
					cityTemps[city] = &[4]int64{temp, 1, minMaxTemp, minMaxTemp}
				} else {
					ct[0] += temp
					ct[1]++
					if temp < ct[2] {
						ct[2] = temp
					}
					if temp > ct[3] {
						ct[3] = temp
					}
				}
				// Reset for the next line
				city = CityName{}
				cityLen = 0
				temp = 0
				isNegative = false
			case '.':
				continue // Skip the decimal point, we know all numbers have 1 decimal place
			case '-':
				isNegative = true // Next number will be negative
			default:
				if cityLen < 44 {
					city[cityLen] = b
					cityLen++
				} else {
					// Inline parsing of temperature
					temp = temp*10 + int64(b-'0')
				}
			}
		}
	}

	results <- cityTemps
}

func main() {
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

	numWorkers := 256
	rawChunks := make(chan []byte, 16384)                   // buffered channel for raw chunks from the file
	validChunks := make(chan []byte, 16384)                 // buffered channel for valid chunks ending with a complete line
	results := make(chan map[CityName]CityTemp, numWorkers) // buffered channel for results from the workers

	// Start the reader
	go reader(file, rawChunks)

	// Start the line supervisor
	go lineSupervisor(rawChunks, validChunks)

	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(validChunks, results)
		}()
	}

	// Collect results
	go func() {
		wg.Wait()
		close(results)
	}()

	finalResults := make(map[CityName]CityTemp)
	for cityTemps := range results {
		for city, ct := range cityTemps {
			finalCt := finalResults[city]
			if finalCt == nil {
				finalCt = new([4]int64)
			}
			finalCt[0] += ct[0]
			finalCt[1] += ct[1]
			if ct[2] < finalCt[2] {
				finalCt[2] = ct[2]
			}
			if ct[3] > finalCt[3] {
				finalCt[3] = ct[3]
			}
			finalResults[city] = finalCt
		}
	}

	// Sort results
	allCities := make([]CityName, 0, len(finalResults))
	for city := range finalResults {
		allCities = append(allCities, city)
	}
	sort.Slice(allCities, func(i, j int) bool {
		return bytes.Compare(allCities[i][:], allCities[j][:]) < 0
	})

	// Calculate and print the final results
	fmt.Print("{")
	for i, city := range allCities {
		ct := finalResults[city]
		fmt.Printf("%s=%.1f/%.1f/%.1f", city[:], float64(ct[2])/10, float64(ct[0])/float64(ct[1])/10, float64(ct[3])/10)
		if i < len(allCities)-1 {
			fmt.Print(", ")
		}
	}
	fmt.Println("}")
}
