package globalpowerplant

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
	Country           string
	Name              string
	CapacityMw        string
	PrimaryFuel       string
	SecondaryFuel     string
	CommissioningYear string
	Owner             string
	Latitude          float64
	Longitude         float64
	Height            float64
	ParseError        error
}

type DescriptionJson struct {
	Country           string `json:"country,omitempty"`
	CapacityMw        string `json:"capacity_mw,omitempty"`
	PrimaryFuel       string `json:"primary_fuel,omitempty"`
	SecondaryFuel     string `json:"secondary_fuel,omitempty"`
	CommissioningYear string `json:"commissioning_year,omitempty"`
	Owner             string `json:"owner,omitempty"`
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

func (e *Entries) ReadAll(ctx context.Context) (chan dataset.Entry, error) {

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	chout := make(chan dataset.Entry, 100)

	go func() {
		defer close(chout)

		// Открываем dataset файл
		f, err := os.Open(e.path)
		if err != nil {
			log.Println(fmt.Errorf("%v", err))
		}
		defer f.Close()

		// Создаём новый CSV reader, читающий записи из открытого файла.
		reader := csv.NewReader(f)
		reader.FieldsPerRecord = 10
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

			csvRecord.parse(record, line, e.path)

			select {
			case <-ctx.Done():
				return
			case chout <- dataset.Entry{
				Name:            csvRecord.Name,
				Description:     csvRecord.Country,
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
	return DescriptionJson{
		Country:           record.Country,
		CapacityMw:        record.CapacityMw,
		PrimaryFuel:       record.PrimaryFuel,
		SecondaryFuel:     record.SecondaryFuel,
		CommissioningYear: record.CommissioningYear,
		Owner:             record.Owner,
	}
}

func (csvRecord *CSVRecord) parse(record []string, line int, path string) {

	// Parse each of the values in the record based on an expected type.
	for idx, value := range record {

		// Parse the value in the record as a string for the string column.
		if idx == 0 {
			if value == "" {
				log.Printf("Parsing file: %v — line %d failed, unexpected type in column %d\n", path, line, idx)
				csvRecord.ParseError = fmt.Errorf("empty string value")
				csvRecord.Country = "untitled"
				continue
			}
			csvRecord.Country = value
			continue
		}

		if idx == 1 {
			if value == "" {
				log.Printf("Parsing file: %v — line %d failed, unexpected type in column %d\n", path, line, idx)
				csvRecord.ParseError = fmt.Errorf("empty string value")
				csvRecord.Name = "untitled"
				continue
			}
			csvRecord.Name = value
			continue
		}

		// Add the float value to the respective field in the CSVRecord.
		switch idx {
		case 2:
			csvRecord.CapacityMw = value
		case 3:
			csvRecord.PrimaryFuel = value
		case 4:
			csvRecord.SecondaryFuel = value
		case 5:
			csvRecord.CommissioningYear = value
		case 6:
			csvRecord.Owner = value
		case 7:
			csvRecord.Latitude = parseCoordinate(idx, line, value)
		case 8:
			csvRecord.Longitude = parseCoordinate(idx, line, value)
		case 9:
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
