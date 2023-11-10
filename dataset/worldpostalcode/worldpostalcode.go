package worldpostalcode

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
	CountryCode string
	PostalCode  string
	PlaceName   string
	AdminName1  string
	AdminCode1  string
	AdminName2  string
	AdminCode2  string
	AdminName3  string
	AdminCode3  string
	Latitude    float64
	Longitude   float64
	Accuracy    string
	Coordinates string
	ParseError  error
}

type DescriptionJson struct {
	CountryCode string `json:"country_code,omitempty"`
	PostalCode  string `json:"postal_code,omitempty"`
	PlaceName   string `json:"place_name,omitempty"`
	AdminName1  string `json:"admin_name_1,omitempty"`
	AdminCode1  string `json:"admin_code_1,omitempty"`
	AdminName2  string `json:"admin_name_2,omitempty"`
	AdminCode2  string `json:"admin_code_2,omitempty"`
	AdminName3  string `json:"admin_name_3,omitempty"`
	AdminCode3  string `json:"admin_code_3,omitempty"`
	Accuracy    string `json:"accuracy,omitempty"`
	Coordinates string `json:"coordinates,omitempty"`
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
		reader.FieldsPerRecord = 13
		reader.Comma = ';'
		//reader.LazyQuotes = true

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
				Name:            csvRecord.PostalCode,
				Description:     getDescription(csvRecord),
				Longitude:       csvRecord.Longitude,
				Latitude:        csvRecord.Latitude,
				Height:          0,
				DescriptionJson: getDescriptionJson(csvRecord),
			}:
			}
			// Increment the line counter.
			line++
		}
	}()

	return chout, nil
}

func getDescription(record CSVRecord) string {
	return fmt.Sprintf("%v (%v)", record.PlaceName, record.CountryCode)
}

func getDescriptionJson(record CSVRecord) DescriptionJson {
	return DescriptionJson{
		CountryCode: record.CountryCode,
		PostalCode:  record.PostalCode,
		PlaceName:   record.PlaceName,
		AdminName1:  record.AdminName1,
		AdminCode1:  record.AdminCode1,
		AdminName2:  record.AdminName2,
		AdminCode2:  record.AdminCode2,
		AdminName3:  record.AdminName3,
		AdminCode3:  record.AdminCode3,
		Accuracy:    record.Accuracy,
		Coordinates: record.Coordinates,
	}
}

func (csvRecord *CSVRecord) parse(record []string, line int, path string) {

	// Parse each of the values in the record based on an expected type.
	for idx, value := range record {

		switch idx {
		case 0:
			csvRecord.CountryCode = value
		case 1:
			csvRecord.PostalCode = value
		case 2:
			csvRecord.PlaceName = value
		case 3:
			csvRecord.AdminName1 = value
		case 4:
			csvRecord.AdminCode1 = value
		case 5:
			csvRecord.AdminName2 = value
		case 6:
			csvRecord.AdminCode2 = value
		case 7:
			csvRecord.AdminName3 = value
		case 8:
			csvRecord.AdminCode3 = value
		case 9:
			csvRecord.Latitude = parseCoordinate(idx, line, value)
		case 10:
			csvRecord.Longitude = parseCoordinate(idx, line, value)
		case 11:
			csvRecord.Accuracy = value
		case 12:
			csvRecord.Coordinates = value
		}
	}
}

func parseCoordinate(idx int, line int, csvField string) float64 {

	floatValue, err := strconv.ParseFloat(strings.TrimSpace(csvField), 64)

	if err != nil {
		log.Printf("Line: %v parsing coordinates %s to float value failed in position %d\n", line, csvField, idx)
		floatValue = 0
	}

	return floatValue
}
