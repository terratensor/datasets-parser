package starter

import (
	"context"
	"github.com/audetv/datasets-parser/app/repos/dataset"
	"github.com/audetv/datasets-parser/app/repos/entity"
	"log"
)

type App struct {
	entries  *dataset.Entries
	entities *entity.Entities
	filename string
}

func NewApp(entries dataset.Store, store entity.Store, filename string) *App {
	app := &App{
		entries:  dataset.NewEntries(entries),
		entities: entity.NewEntities(store),
		filename: filename,
	}
	return app
}

func (a App) Process(ctx context.Context) {
	chin, err := a.entries.ReadAll(ctx)
	if err != nil {
		log.Panic(err)
	}

	var entries []dataset.Entry
	for {
		entry, ok := <-chin
		if !ok {
			break // exit break loop
		} else {
			entries = append(entries, entry)
			en := entity.Entity{
				Filename:        a.filename,
				Name:            entry.Name,
				Description:     entry.Description,
				Longitude:       entry.Longitude,
				Latitude:        entry.Latitude,
				Height:          entry.Height,
				DescriptionJson: entry.DescriptionJson,
			}

			_, err := a.entities.Create(ctx, en)
			if err != nil {
				return
			}
		}
	}

	//log.Println(entries)
}
