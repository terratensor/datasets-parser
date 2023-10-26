package starter

import (
	"context"
	"github.com/audetv/datasets-parser/app/repos/dataset"
	"github.com/audetv/datasets-parser/app/repos/entity"
)

type App struct {
	entries  *dataset.Entries
	entities *entity.Entities
}

func NewApp(entries dataset.Store, store entity.Store) *App {
	app := &App{
		entries:  dataset.NewEntries(entries),
		entities: entity.NewEntities(store),
	}
	return app
}

func (a App) Process(ctx context.Context) {

}
