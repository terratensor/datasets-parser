package starter

import (
	"context"
	"github.com/audetv/datasets-parser/app/repos/dataset"
	"github.com/audetv/datasets-parser/app/repos/entity"
	"github.com/google/uuid"
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

	var entities []entity.Entity
	batchSize := 3500
	batchSizeCount := 0

	for {
		entry, ok := <-chin
		if !ok {
			break // exit break loop
		} else {
			en := entity.Entity{
				ID:              uuid.New(),
				Filename:        a.filename,
				Name:            entry.Name,
				Description:     entry.Description,
				Longitude:       entry.Longitude,
				Latitude:        entry.Latitude,
				Height:          entry.Height,
				DescriptionJson: entry.DescriptionJson,
			}

			entities = append(entities, en)
			batchSizeCount++
			//_, err := a.entities.Create(ctx, en)
			//if err != nil {
			//	return
			//}
		}

		// Записываем пакетам по batchSize параграфов
		if batchSizeCount == batchSize-1 {
			err = a.entities.BulkInsert(ctx, entities, len(entities))
			if err != nil {
				log.Printf("log bulk insert error query: %v \r\n", err)
			}
			// очищаем slice
			entities = nil
			batchSizeCount = 0
		}
	}

	//log.Println(entries)
}
