package graphql

import (
	"github.com/sarna/worb/internal/store"
)

type Resolver struct {
	Store *store.DB
}
