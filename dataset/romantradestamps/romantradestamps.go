package romantradestamps

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
	Name       string
	Site       string
	Code       string
	Type       string
	Latitude   float64
	Longitude  float64
	Height     float64
	ParseError error
}

type DescriptionJson struct {
	Site string `json:"site,omitempty"`
	Code string `json:"code,omitempty"`
	Type string `json:"type,omitempty"`
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
		reader.FieldsPerRecord = 6
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
				Name:            csvRecord.Name,
				Description:     csvRecord.Site,
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
		Site: record.Site,
		Code: record.Code,
		Type: record.Type,
	}
}

func (csvRecord *CSVRecord) parse(record []string, line int, path string) {

	// Parse each of the values in the record based on an expected type.
	for idx, value := range record {

		switch idx {
		case 0:
			csvRecord.Name = value
		case 1:
			csvRecord.Site = value
		case 2:
			csvRecord.Code = value
		case 3:
			csvRecord.Type = value
		case 4:
			csvRecord.Latitude = parseCoordinate(idx, line, value)
		case 5:
			csvRecord.Longitude = parseCoordinate(idx, line, value)
		case 6:
			csvRecord.Height = parseCoordinate(idx, line, value)
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
