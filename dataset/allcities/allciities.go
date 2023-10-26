package allcities

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/audetv/datasets-parser/app/repos/dataset"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

var _ dataset.Store = &Entries{}

type CSVRecord struct {
	Name           string
	AlternateNames string
	Elevation      string
	Country        string
	Population     string
	Latitude       float64
	Longitude      float64
	Height         float64
	ParseError     error
}

type DescriptionJson struct {
	Elevation  int    `json:"elevation,omitempty"`
	Country    string `json:"country,omitempty"`
	Population int    `json:"population,omitempty"`
}

type Entries struct {
	path string
}

func NewCSVEntries(path string) (*Entries, error) {
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	es := &Entries{
		path: path,
	}

	return es, nil
}

func (nd *Entries) ReadAll(ctx context.Context) (chan dataset.Entry, error) {

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	chout := make(chan dataset.Entry, 100)

	go func() {
		defer close(chout)

		// Открываем dataset файл
		f, err := os.Open(nd.path)
		if err != nil {
			log.Println(fmt.Errorf("%v", err))
		}
		defer f.Close()

		// Создаём новый CSV reader, читающий записи из открытого файла.
		reader := csv.NewReader(f)
		reader.FieldsPerRecord = 8
		reader.Comma = ';'

		// line will help us keep track of line number for logging.
		line := 0

		// Read in the records looking for unexpected types.
		for {
			// Read in a row. Check if we are at the end of the file.
			record, err := reader.Read()
			if err == io.EOF {
				break
			}

			if line == 0 {
				line++
				continue
			}

			// Create a CSVRecord value for the row.
			var csvRecord CSVRecord

			csvRecord.parse(record, line)

			select {
			case <-ctx.Done():
				return
			case chout <- dataset.Entry{
				Name:            csvRecord.Name,
				Description:     csvRecord.AlternateNames,
				Longitude:       csvRecord.Longitude,
				Latitude:        csvRecord.Latitude,
				Height:          csvRecord.Height,
				DescriptionJson: getDescriptionJson(csvRecord),
			}:
			}
			// Increment the line counter.
			line++
		}
	}()

	return chout, nil
}

func getDescriptionJson(record CSVRecord) DescriptionJson {
	elevation, _ := strconv.Atoi(record.Elevation)
	population, _ := strconv.Atoi(record.Population)

	return DescriptionJson{
		Elevation:  elevation,
		Country:    record.Country,
		Population: population,
	}
}

func (csvRecord *CSVRecord) parse(record []string, line int) {

	// Parse each of the values in the record based on an expected type.
	for idx, value := range record {

		// Parse the value in the record as a string for the string column.
		if idx == 0 {
			if value == "" {
				log.Printf("Parsing line %d failed, unexpected type in column %d\n", line, idx)
				csvRecord.ParseError = fmt.Errorf("empty string value")
				break
			}
			csvRecord.Name = value
			continue
		}

		// Add the float value to the respective field in the CSVRecord.
		switch idx {
		case 1:
			csvRecord.AlternateNames = value
		case 2:
			csvRecord.Elevation = value
		case 3:
			csvRecord.Country = value
		case 4:
			csvRecord.Population = value
		case 5:
			csvRecord.Latitude = parseCoordinate(idx, line, value)
		case 6:
			csvRecord.Longitude = parseCoordinate(idx, line, value)
		case 7:
			csvRecord.Height = parseCoordinate(idx, line, value)
		}
	}
}

func parseCoordinate(idx int, line int, csvField string) float64 {

	floatValue, err := strconv.ParseFloat(strings.TrimSpace(csvField), 64)

	if err != nil {
		log.Printf("Line: %v parsing coordinates %s to float value failed in position %d\n", line, csvField, idx)
	}

	return floatValue
}
