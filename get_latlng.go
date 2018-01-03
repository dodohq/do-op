package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
)

type location struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type googleResponse struct {
	Results []*struct {
		Geometry *struct {
			Location *location `json:"location"`
		} `json:"geometry"`
	} `json:"results"`
}

func printUsage() {
	fmt.Println("Usage: get_latlng <csv filepath> <number of column containing zipcode>")
	fmt.Println("Number of column is counting from 0")
}

func main() {
	args := os.Args[1:]
	if len(args) <= 1 {
		log.Fatal("Error: Not enough arguments provided")
		printUsage()
		os.Exit(1)
	}

	fileName := args[0]
	fileReader, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Error: Invalid file")
		os.Exit(1)
	}
	defer fileReader.Close()

	colNum, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatal("Error: invalid column ")
		os.Exit(1)

	}

	csvReader := csv.NewReader(fileReader)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err.Error())
		os.Exit(1)
	}
	if len(records) <= 1 {
		log.Fatal("Error: Empty record")
		os.Exit(1)
	}

	resultChannels := []chan location{}
	for i, record := range records {
		if i == 0 {
			resultChannels = append(resultChannels, make(chan location))
			continue
		}
		resultChannels = append(resultChannels, make(chan location))
		defer close(resultChannels[i])
		if len(record) <= colNum {
			errLatLng := location{-1, -1}
			resultChannels[i] <- errLatLng
			continue
		}
		postalCode := record[colNum]
		go func(channel chan<- location) {
			res, err := http.Get("http://maps.googleapis.com/maps/api/geocode/json?address=" + postalCode)
			if err != nil {
				errLatLng := location{-1, -1}
				channel <- errLatLng
				return
			}
			defer res.Body.Close()

			b, err := ioutil.ReadAll(res.Body)
			if err != nil {
				errLatLng := location{-1, -1}
				channel <- errLatLng
				return
			}

			var resp googleResponse
			err = json.Unmarshal(b, &resp)
			if err != nil {
				fmt.Println(err.Error())
				errLatLng := location{-1, -1}
				channel <- errLatLng
				return
			}

			latLng := location{resp.Results[0].Geometry.Location.Lat, resp.Results[0].Geometry.Location.Lng}
			channel <- latLng
		}(resultChannels[i])
	}

	for i := range records {
		if i == 0 {
			records[i] = append(records[i], "Lat")
			records[i] = append(records[i], "Lng")
			continue
		}
		location := <-resultChannels[i]
		records[i] = append(records[i], strconv.FormatFloat(location.Lat, 'f', 6, 64))
		records[i] = append(records[i], strconv.FormatFloat(location.Lng, 'f', 6, 64))
	}

	resultFile, err := os.OpenFile("data.csv", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal("Error: Fail to open output file")
		os.Exit(1)
	}

	csvWriter := csv.NewWriter(resultFile)
	csvWriter.WriteAll(records)
}
