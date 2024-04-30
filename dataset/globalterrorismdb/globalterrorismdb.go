package globalterrorismdb

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
	Eventid       string
	Iyear         string
	Imonth        string
	Iday          string
	Location      string
	Summary       string
	Targets       string
	AttacktypeTxt string
	Terrorists    string
	MotiveClaime  string
	Weapons       string
	Damage        string
	Killed        string
	Wounded       string
	Latitude      float64
	Longitude     float64
	Height        float64
	ParseError    error
}

type DescriptionJson struct {
	Eventid       string `json:"eventid,omitempty"`
	Iyear         string `json:"iyear,omitempty"`
	Imonth        string `json:"imonth,omitempty"`
	Iday          string `json:"iday,omitempty"`
	Targets       string `json:"targets,omitempty"`
	AttacktypeTxt string `json:"attacktype_txt,omitempty"`
	Terrorists    string `json:"terrorists,omitempty"`
	MotiveClaime  string `json:"motive_claime,omitempty"`
	Weapons       string `json:"weapons,omitempty"`
	Damage        string `json:"damage,omitempty"`
	Killed        string `json:"killed,omitempty"`
	Wounded       string `json:"wounded,omitempty"`
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
				Name:            csvRecord.Location,
				Description:     csvRecord.Summary,
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
		Eventid:       record.Eventid,
		Iyear:         record.Iyear,
		Imonth:        record.Imonth,
		Iday:          record.Iday,
		Targets:       record.Targets,
		AttacktypeTxt: record.AttacktypeTxt,
		Terrorists:    record.Terrorists,
		MotiveClaime:  record.MotiveClaime,
		Weapons:       record.Weapons,
		Damage:        record.Damage,
		Killed:        record.Killed,
		Wounded:       record.Wounded,
	}
}

func (csvRecord *CSVRecord) parse(record []string, line int, path string) {

	// Parse each of the values in the record based on an expected type.
	for idx, value := range record {

		switch idx {
		case 0:
			csvRecord.Eventid = value
		case 1:
			csvRecord.Iyear = value
		case 2:
			csvRecord.Imonth = value
		case 3:
			csvRecord.Iday = value
		case 4:
			csvRecord.Location = value
		case 5:
			csvRecord.Summary = value
		case 6:
			csvRecord.Targets = value
		case 7:
			csvRecord.AttacktypeTxt = value
		case 8:
			csvRecord.Terrorists = value
		case 9:
			csvRecord.MotiveClaime = value
		case 10:
			csvRecord.Weapons = value
		case 11:
			csvRecord.Damage = value
		case 12:
			csvRecord.Killed = value
		case 13:
			csvRecord.Wounded = value
		case 14:
			csvRecord.Latitude = parseCoordinate(idx, line, value)
		case 15:
			csvRecord.Longitude = parseCoordinate(idx, line, value)
		case 16:
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
