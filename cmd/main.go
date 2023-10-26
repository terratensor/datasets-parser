package main

import (
	"context"
	"github.com/audetv/datasets-parser/app/repos/dataset"
	"github.com/audetv/datasets-parser/app/repos/entity"
	"github.com/audetv/datasets-parser/app/starter"
	"github.com/audetv/datasets-parser/dataset/bibleplaces"
	"github.com/audetv/datasets-parser/db/entitystore"
	"log"
	"os"
	"os/signal"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	path := "data/all-bible-places.csv"

	dsn := "host=localhost user=app password=secret dbname=geomatrix port=54325 sslmode=disable TimeZone=Europe/Moscow"
	log.Println("подготовка соединения с базой данных")

	var entityStore entity.Store
	dbEntityStore, err := entitystore.NewEntities(dsn)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("успешно завершено")
	entityStore = dbEntityStore

	var entries dataset.Store
	entries, err = bibleplaces.NewCSVEntries(path)

	app := starter.NewApp(entries, entityStore)

	app.Process(ctx)

	log.Println("Done!")
}
