package entitystore

import (
	"context"
	"github.com/audetv/datasets-parser/app/repos/entity"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"time"
)

type DBEntities []*DBEntity

type DBEntity struct {
	ID              uuid.UUID `gorm:"type:uuid"`
	Filename        string
	Name            string
	Description     string
	Longitude       float64     `gorm:"type:double precision"`
	Latitude        float64     `gorm:"type:double precision"`
	Height          float64     `gorm:"type:double precision"`
	DescriptionJson interface{} `gorm:"type:json"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
	DeletedAt       *time.Time
}

type Entities struct {
	db *gorm.DB
}

var _ entity.Store = &Entities{}

func NewEntities(dsn string) (*Entities, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})

	err = db.AutoMigrate(&DBEntity{})
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}
	bs := &Entities{
		db: db,
	}
	return bs, nil
}

func (es *Entities) Create(ctx context.Context, e entity.Entity) error {
	dbEntity := DBEntity{
		ID:              e.ID,
		Name:            e.Name,
		Filename:        e.Filename,
		Description:     e.Description,
		Longitude:       e.Longitude,
		Latitude:        e.Latitude,
		Height:          e.Height,
		DescriptionJson: e.DescriptionJson,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
		DeletedAt:       nil,
	}

	result := es.db.WithContext(ctx).Create(&dbEntity)

	return result.Error
}

func (es *Entities) BulkInsert(ctx context.Context, entities []entity.Entity, batchSize int) error {
	var dbEnts DBEntities
	for _, e := range entities {
		dbEntity := DBEntity{
			ID:              e.ID,
			Name:            e.Name,
			Filename:        e.Filename,
			Description:     e.Description,
			Longitude:       e.Longitude,
			Latitude:        e.Latitude,
			Height:          e.Height,
			DescriptionJson: e.DescriptionJson,
			CreatedAt:       time.Now(),
			UpdatedAt:       time.Now(),
			DeletedAt:       nil,
		}
		dbEnts = append(dbEnts, &dbEntity)
	}
	result := es.db.WithContext(ctx).CreateInBatches(dbEnts, batchSize)
	return result.Error
}
