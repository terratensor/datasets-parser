package main

import (
	"context"
	"github.com/audetv/datasets-parser/app/repos/entity"
	"github.com/audetv/datasets-parser/app/starter"
	"github.com/audetv/datasets-parser/db/entitystore"
	flag "github.com/spf13/pflag"
	"log"
	"os"
	"os/signal"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	var dataPath string

	flag.StringVarP(
		&dataPath,
		"data",
		"d",
		"./data/",
		"путь до папки с файлами для обработки",
	)
	flag.Parse()

	dsn := "host=localhost user=app password=secret dbname=geomatrix port=54325 sslmode=disable TimeZone=Europe/Moscow"
	log.Println("подготовка соединения с базой данных")

	var entityStore entity.Store
	dbEntityStore, err := entitystore.NewEntities(dsn)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("успешно завершено")
	entityStore = dbEntityStore

	app := starter.NewApp(entityStore)
	app.Process(ctx, dataPath)

	log.Println("Done!")
}
