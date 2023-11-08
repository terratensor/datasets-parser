package pleiades

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
	Title       string
	Description string
	Details     string
	Provenance  string
	Uri         string
	Latitude    float64
	Longitude   float64
	Height      float64
	ParseError  error
}

type DescriptionJson struct {
	Details    string `json:"details,omitempty"`
	Provenance string `json:"provenance,omitempty"`
	Uri        string `json:"uri,omitempty"`
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
		reader.FieldsPerRecord = 7
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
				Name:            csvRecord.Title,
				Description:     csvRecord.Description,
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
		Details:    record.Details,
		Provenance: record.Provenance,
		Uri:        record.Uri,
	}
}

func (csvRecord *CSVRecord) parse(record []string, line int, path string) {

	// Parse each of the values in the record based on an expected type.
	for idx, value := range record {

		switch idx {
		case 0:
			csvRecord.Title = value
		case 1:
			csvRecord.Description = value
		case 2:
			csvRecord.Details = value
		case 3:
			csvRecord.Provenance = value
		case 4:
			csvRecord.Uri = value
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
		floatValue = 0
	}

	return floatValue
}
