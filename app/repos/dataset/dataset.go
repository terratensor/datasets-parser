package dataset

import "context"

// Entry преобразованная запись файла
type Entry struct {
	Name            string
	Description     string
	Longitude       float64
	Latitude        float64
	Height          float64
	DescriptionJson interface{}
}

type Store interface {
	ReadAll(ctx context.Context) (chan Entry, error)
}

type Entries struct {
	store Store
}

func NewEntries(store Store) *Entries {
	return &Entries{
		store: store,
	}
}

func (es *Entries) ReadAll(ctx context.Context) (chan Entry, error) {
	chin, err := es.store.ReadAll(ctx)
	if err != nil {
		return nil, err
	}
	chout := make(chan Entry, 100)
	go func() {
		defer close(chout)
		for {
			select {
			case <-ctx.Done():
				return
			case entry, ok := <-chin:
				if !ok {
					return
				}
				chout <- entry
			}
		}
	}()
	return chout, nil
}
