package kvtests

import (
	"context"

	"github.com/bvkgo/kv"
)

type Backend struct {
	NewTx func(context.Context) (kv.Transaction, error)
	NewIt func(context.Context) (kv.Iterator, error)
}
