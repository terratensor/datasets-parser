package volcanic

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
	VolcanoName              string
	Year                     string
	Month                    string
	Day                      string
	Elevation                string
	VolcanoType              string
	Status                   string
	Location                 string
	Country                  string
	FlagTsunami              string
	FlagEarthquake           string
	VolcanicExplosivityIndex string
	Deaths                   string
	Missing                  string
	Injuries                 string
	Damage                   string
	DamageDescription        string
	HousesDestroyed          string
	Lat                      float64
	Lon                      float64
	Height                   float64
	ParseError               error
}

type DescriptionJson struct {
	VolcanoName              string `json:"volcano_name,omitempty"`
	Year                     string `json:"year,omitempty"`
	Month                    string `json:"month,omitempty"`
	Day                      string `json:"day,omitempty"`
	Elevation                string `json:"elevation,omitempty"`
	VolcanoType              string `json:"volcano_type,omitempty"`
	Status                   string `json:"status,omitempty"`
	Location                 string `json:"location,omitempty"`
	Country                  string `json:"country,omitempty"`
	FlagTsunami              string `json:"flag_tsunami,omitempty"`
	FlagEarthquake           string `json:"flag_earthquake,omitempty"`
	VolcanicExplosivityIndex string `json:"volcanic_explosivity_index,omitempty"`
	Deaths                   string `json:"deaths,omitempty"`
	Missing                  string `json:"missing,omitempty"`
	Injuries                 string `json:"injuries,omitempty"`
	Damage                   string `json:"damage,omitempty"`
	DamageDescription        string `json:"damage_description,omitempty"`
	HousesDestroyed          string `json:"houses_destroyed,omitempty"`
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
				Name:            csvRecord.VolcanoName,
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
	return fmt.Sprintf("%v — %v", record.Country, record.Location)
}

func getDescriptionJson(record CSVRecord) DescriptionJson {
	return DescriptionJson{
		VolcanoName:              record.VolcanoName,
		Year:                     record.Year,
		Month:                    record.Month,
		Day:                      record.Day,
		Elevation:                record.Elevation,
		VolcanoType:              record.VolcanoType,
		Status:                   record.Status,
		Location:                 record.Location,
		Country:                  record.Country,
		FlagTsunami:              record.FlagTsunami,
		FlagEarthquake:           record.FlagEarthquake,
		VolcanicExplosivityIndex: record.VolcanicExplosivityIndex,
		Deaths:                   record.Deaths,
		Missing:                  record.Missing,
		Injuries:                 record.Injuries,
		Damage:                   record.Damage,
		DamageDescription:        record.DamageDescription,
		HousesDestroyed:          record.HousesDestroyed,
	}
}

func (csvRecord *CSVRecord) parse(record []string, line int, path string) {

	// Parse each of the values in the record based on an expected type.
	for idx, value := range record {

		switch idx {
		case 0:
			csvRecord.VolcanoName = value
		case 1:
			csvRecord.Year = value
		case 2:
			csvRecord.Month = value
		case 3:
			csvRecord.Day = value
		case 4:
			csvRecord.Elevation = value
		case 5:
			csvRecord.VolcanoType = value
		case 6:
			csvRecord.Status = value
		case 7:
			csvRecord.Location = value
		case 8:
			csvRecord.Country = value
		case 9:
			csvRecord.FlagTsunami = value
		case 10:
			csvRecord.FlagEarthquake = value
		case 11:
			csvRecord.VolcanicExplosivityIndex = value
		case 12:
			csvRecord.Deaths = value
		case 13:
			csvRecord.Missing = value
		case 14:
			csvRecord.Injuries = value
		case 15:
			csvRecord.Damage = value
		case 16:
			csvRecord.DamageDescription = value
		case 17:
			csvRecord.HousesDestroyed = value
		case 18:
			csvRecord.Lat = parseCoordinate(idx, line, value)
		case 19:
			csvRecord.Lon = parseCoordinate(idx, line, value)
		case 20:
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
