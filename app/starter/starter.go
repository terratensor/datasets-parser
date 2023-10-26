package starter

import (
	"context"
	"fmt"
	"github.com/audetv/datasets-parser/app/repos/dataset"
	"github.com/audetv/datasets-parser/app/repos/entity"
	"github.com/audetv/datasets-parser/dataset/allcities"
	"github.com/audetv/datasets-parser/dataset/bibleplaces"
	"github.com/google/uuid"
	"log"
	"os"
)

type App struct {
	entities *entity.Entities
}

func NewApp(store entity.Store) *App {
	app := &App{
		entities: entity.NewEntities(store),
	}
	return app
}

func (a *App) parseDataset(ctx context.Context, entries dataset.Store, filename string) {

	chin, err := entries.ReadAll(ctx)
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
				Filename:        filename,
				Name:            entry.Name,
				Description:     entry.Description,
				Longitude:       entry.Longitude,
				Latitude:        entry.Latitude,
				Height:          entry.Height,
				DescriptionJson: entry.DescriptionJson,
			}

			entities = append(entities, en)
			batchSizeCount++
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
}

func (a *App) Process(ctx context.Context) {

	folder := "data"
	// читаем все файлы в директории
	files, err := os.ReadDir(folder)
	if err != nil {
		log.Fatal(err)
	}

	var entries dataset.Store

	// итерируемся по списку файлов
	for _, file := range files {
		if file.IsDir() == false {
			// если файл gitignore, то ничего не делаем пропускаем и продолжаем цикл
			if file.Name() == ".gitignore" {
				continue
			}

			entries, err = getEntriesInstance(entries, folder, file.Name())
			if err != nil {
				log.Println(err)
				continue
			}
			a.parseDataset(ctx, entries, file.Name())
		}
	}
}

func getEntriesInstance(entries dataset.Store, folder string, filename string) (dataset.Store, error) {

	switch filename {
	case "all-bible-places.csv":
		ne, err := bibleplaces.NewCSVEntries(fmt.Sprintf("%v/%v", folder, filename))
		if err != nil {
			return nil, err
		}
		return ne, nil
	case "utf8.all-cities-with-a-population.csv":
		ne, err := allcities.NewCSVEntries(fmt.Sprintf("%v/%v", folder, filename))
		if err != nil {
			return nil, err
		}
		return ne, nil
	default:
		return nil, fmt.Errorf("%v file not supported", filename)
	}
}
