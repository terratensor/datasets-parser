package unesco

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
	Description    string
	Justification  string
	DateInscribed  string
	SecondaryDates string
	Danger         string
	Dated          string
	DangerList     string
	AreaHectares   string
	StatesName     string
	Region         string
	Category       string
	Longitude      float64
	Latitude       float64
	Height         float64
	ParseError     error
}

type DescriptionJson struct {
	Name           string `json:"name,omitempty"`
	Description    string `json:"description,omitempty"`
	Justification  string `json:"justification,omitempty"`
	DateInscribed  string `json:"date_inscribed,omitempty"`
	SecondaryDates string `json:"secondary_dates,omitempty"`
	Danger         string `json:"danger,omitempty"`
	Dated          string `json:"dated,omitempty"`
	DangerList     string `json:"danger_list,omitempty"`
	AreaHectares   string `json:"area_hectares,omitempty"`
	StatesName     string `json:"states_name,omitempty"`
	Region         string `json:"region,omitempty"`
	Category       string `json:"category,omitempty"`
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
		reader.FieldsPerRecord = 15
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
				Description:     getDescription(csvRecord),
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

func getDescription(record CSVRecord) string {
	return fmt.Sprintf("%v", record.Description)
}

func getDescriptionJson(record CSVRecord) DescriptionJson {
	return DescriptionJson{
		Name:           record.Name,
		Description:    record.Description,
		Justification:  record.Justification,
		DateInscribed:  record.DateInscribed,
		SecondaryDates: record.SecondaryDates,
		Danger:         record.Danger,
		Dated:          record.Dated,
		DangerList:     record.DangerList,
		AreaHectares:   record.AreaHectares,
		StatesName:     record.StatesName,
		Region:         record.Region,
		Category:       record.Category,
	}
}

func (csvRecord *CSVRecord) parse(record []string, line int, path string) {

	// Parse each of the values in the record based on an expected type.
	for idx, value := range record {

		switch idx {
		case 0:
			csvRecord.Name = value
		case 1:
			csvRecord.Description = value
		case 2:
			csvRecord.Justification = value
		case 3:
			csvRecord.DateInscribed = value
		case 4:
			csvRecord.SecondaryDates = value
		case 5:
			csvRecord.Danger = value
		case 6:
			csvRecord.Dated = value
		case 7:
			csvRecord.DangerList = value
		case 8:
			csvRecord.AreaHectares = value
		case 9:
			csvRecord.StatesName = value
		case 10:
			csvRecord.Region = value
		case 11:
			csvRecord.Category = value
		case 12:
			csvRecord.Latitude = parseCoordinate(idx, line, value)
		case 13:
			csvRecord.Longitude = parseCoordinate(idx, line, value)
		case 14:
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
