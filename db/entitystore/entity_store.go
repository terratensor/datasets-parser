package entitystore

import (
	"context"
	"github.com/audetv/datasets-parser/app/repos/entity"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"time"
)

type DBEntity struct {
	ID              uuid.UUID `gorm:"type:uuid"`
	Filename        string
	Name            string
	Description     string
	Longitude       float64
	Latitude        float64
	Height          float64
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

func (es *Entities) Create(ctx context.Context, b entity.Entity) error {
	dbEntity := DBEntity{
		ID:              b.ID,
		Name:            b.Name,
		Filename:        b.Filename,
		Description:     b.Description,
		Longitude:       b.Longitude,
		Latitude:        b.Latitude,
		Height:          b.Height,
		DescriptionJson: b.DescriptionJson,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
		DeletedAt:       nil,
	}

	result := es.db.WithContext(ctx).Create(&dbEntity)

	return result.Error
}
