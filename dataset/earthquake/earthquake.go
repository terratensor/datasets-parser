package earthquake

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
	LocationName       string
	Year               string
	Month              string
	Day                string
	Country            string
	FocalDepth         string
	EQPrimary          string
	FlagTsunami        string
	Deaths             string
	Injuries           string
	Missing            string
	MissingDescription string
	HousesDestroyed    string
	HousesDamaged      string
	Damage             string
	DamageDescription  string
	Lat                float64
	Lon                float64
	Height             float64
	ParseError         error
}

type DescriptionJson struct {
	LocationName       string `json:"location_name,omitempty"`
	Year               string `json:"year,omitempty"`
	Month              string `json:"month,omitempty"`
	Day                string `json:"day,omitempty"`
	Country            string `json:"country,omitempty"`
	FocalDepth         string `json:"focal_depth,omitempty"`
	EQPrimary          string `json:"eq_primary,omitempty"`
	FlagTsunami        string `json:"flag_tsunami,omitempty"`
	Deaths             string `json:"deaths,omitempty"`
	Injuries           string `json:"injuries,omitempty"`
	Missing            string `json:"missing,omitempty"`
	MissingDescription string `json:"missing_description,omitempty"`
	HousesDestroyed    string `json:"houses_destroyed,omitempty"`
	HousesDamaged      string `json:"houses_damaged,omitempty"`
	Damage             string `json:"damage,omitempty"`
	DamageDescription  string `json:"damage_description,omitempty"`
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
		reader.FieldsPerRecord = 19
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
				Name:            csvRecord.LocationName,
				Description:     getDescription(csvRecord),
				Longitude:       csvRecord.Lon,
				Latitude:        csvRecord.Lat,
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
	return fmt.Sprintf("%v", record.Country)
}

func getDescriptionJson(record CSVRecord) DescriptionJson {
	return DescriptionJson{
		LocationName:       record.LocationName,
		Year:               record.Year,
		Month:              record.Month,
		Day:                record.Day,
		Country:            record.Country,
		FocalDepth:         record.FocalDepth,
		EQPrimary:          record.EQPrimary,
		FlagTsunami:        record.FlagTsunami,
		Deaths:             record.Deaths,
		Injuries:           record.Injuries,
		Missing:            record.Missing,
		MissingDescription: record.MissingDescription,
		HousesDestroyed:    record.HousesDestroyed,
		HousesDamaged:      record.HousesDamaged,
		Damage:             record.Damage,
		DamageDescription:  record.DamageDescription,
	}
}

func (csvRecord *CSVRecord) parse(record []string, line int, path string) {

	// Parse each of the values in the record based on an expected type.
	for idx, value := range record {

		switch idx {
		case 0:
			csvRecord.LocationName = value
		case 1:
			csvRecord.Year = value
		case 2:
			csvRecord.Month = value
		case 3:
			csvRecord.Day = value
		case 4:
			csvRecord.Country = value
		case 5:
			csvRecord.FocalDepth = value
		case 6:
			csvRecord.EQPrimary = value
		case 7:
			csvRecord.FlagTsunami = value
		case 8:
			csvRecord.Deaths = value
		case 9:
			csvRecord.Injuries = value
		case 10:
			csvRecord.Missing = value
		case 11:
			csvRecord.MissingDescription = value
		case 12:
			csvRecord.HousesDestroyed = value
		case 13:
			csvRecord.HousesDamaged = value
		case 14:
			csvRecord.Damage = value
		case 15:
			csvRecord.DamageDescription = value
		case 16:
			csvRecord.Lat = parseCoordinate(idx, line, value)
		case 17:
			csvRecord.Lon = parseCoordinate(idx, line, value)
		case 18:
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
