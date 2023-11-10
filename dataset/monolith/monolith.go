package monolith

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
	NumID               string
	Name                string
	Created             string
	Disappeared         string
	Accuracy            string
	Description         string
	Construction        string
	Notes               string
	MonolithImage       string
	MonolithImageSecond string
	Spotted             string
	MainLink            string
	SupportLinks        string
	Latitude            float64
	Longitude           float64
	Height              float64
	Geohash             string
	ParseError          error
}

type DescriptionJson struct {
	NumID               string `json:"num_id,omitempty"`
	Created             string `json:"created,omitempty"`
	Disappeared         string `json:"disappeared,omitempty"`
	Accuracy            string `json:"accuracy,omitempty"`
	Construction        string `json:"construction,omitempty"`
	Notes               string `json:"notes,omitempty"`
	MonolithImage       string `json:"monolith_image,omitempty"`
	MonolithImageSecond string `json:"monolith_image_second,omitempty"`
	Spotted             string `json:"spotted,omitempty"`
	MainLink            string `json:"main_link,omitempty"`
	SupportLinks        string `json:"support_links,omitempty"`
	Geohash             string `json:"geohash,omitempty"`
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
		reader.FieldsPerRecord = 17
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
		NumID:               record.NumID,
		Created:             record.Created,
		Disappeared:         record.Disappeared,
		Accuracy:            record.Accuracy,
		Construction:        record.Construction,
		Notes:               record.Notes,
		MonolithImage:       record.MonolithImage,
		MonolithImageSecond: record.MonolithImageSecond,
		Spotted:             record.Spotted,
		MainLink:            record.MainLink,
		SupportLinks:        record.SupportLinks,
		Geohash:             record.Geohash,
	}
}

func (csvRecord *CSVRecord) parse(record []string, line int, path string) {

	// Parse each of the values in the record based on an expected type.
	for idx, value := range record {

		switch idx {
		case 0:
			csvRecord.NumID = value
		case 1:
			csvRecord.Name = value
		case 2:
			csvRecord.Created = value
		case 3:
			csvRecord.Disappeared = value
		case 4:
			csvRecord.Accuracy = value
		case 5:
			csvRecord.Description = value
		case 6:
			csvRecord.Construction = value
		case 7:
			csvRecord.Notes = value
		case 8:
			csvRecord.MonolithImage = value
		case 9:
			csvRecord.MonolithImageSecond = value
		case 10:
			csvRecord.Spotted = value
		case 11:
			csvRecord.MainLink = value
		case 12:
			csvRecord.SupportLinks = value
		case 13:
			csvRecord.Latitude = parseCoordinate(idx, line, value)
		case 14:
			csvRecord.Longitude = parseCoordinate(idx, line, value)
		case 15:
			csvRecord.Height = parseCoordinate(idx, line, value)
		case 16:
			csvRecord.Geohash = value
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
