package entity

import (
	"context"
	"fmt"
	"github.com/google/uuid"
)

// Entity сущность
type Entity struct {
	ID              uuid.UUID
	Filename        string
	Name            string
	Description     string
	Longitude       float64
	Latitude        float64
	Height          float64
	DescriptionJson interface{}
	CellID          string
}

type Store interface {
	Create(ctx context.Context, e Entity) error
	BulkInsert(ctx context.Context, entities []Entity, batchSize int) error
}

type Entities struct {
	store Store
}

func NewEntities(store Store) *Entities {
	return &Entities{
		store,
	}
}

func (es *Entities) Create(ctx context.Context, e Entity) (*Entity, error) {
	e.ID = uuid.New()
	err := es.store.Create(ctx, e)
	if err != nil {
		return nil, fmt.Errorf("create entity error: %w", err)
	}
	return &e, nil
}

func (es *Entities) BulkInsert(ctx context.Context, entities []Entity, batchSize int) error {
	err := es.store.BulkInsert(ctx, entities, batchSize)
	if err != nil {
		return fmt.Errorf("entities batch insert error: %w", err)
	}
	return nil
}
