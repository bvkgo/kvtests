package kvtests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/bvkgo/kv"
)

type Options struct {
	NewTx func(context.Context) (kv.Transaction, error)
	NewIt func(context.Context) (kv.Iterator, error)

	Seed int64

	NumItems int

	rand *rand.Rand
}

func (opts *Options) setDefaults() {
	if opts.NumItems == 0 {
		opts.NumItems = 1000
	}
	if opts.Seed == 0 {
		opts.Seed = time.Now().UnixNano()
	}
	opts.rand = rand.New(rand.NewSource(opts.Seed))
}

func (opts *Options) Check() error {
	if opts.NewTx == nil || opts.NewIt == nil {
		return fmt.Errorf("NewTx and NewIt fields are required: %w", os.ErrInvalid)
	}
	return nil
}

func (opts *Options) getItem(i int) string {
	return fmt.Sprintf("%08d", i)
}

func (opts *Options) getKeyPair() [2]string {
	k1 := opts.getItem(opts.rand.Intn(opts.NumItems))
	k2 := opts.getItem(opts.rand.Intn(opts.NumItems))
	for k2 == k1 {
		k2 = opts.getItem(opts.rand.Intn(opts.NumItems))
	}
	return [2]string{k1, k2}
}

func (opts *Options) getKeyPairs(npairs int) ([][2]string, error) {
	if opts.NumItems < 2*npairs*2 {
		return nil, fmt.Errorf("%d random key pairs need %d items", 2*npairs*2)
	}

	var keyPairs [][2]string
	keyMap := make(map[string]int)
	for i := 0; i < npairs; i++ {
		k1 := opts.getItem(opts.rand.Intn(opts.NumItems))
		for _, ok := keyMap[k1]; ok; {
			k1 = opts.getItem(opts.rand.Intn(opts.NumItems))
		}
		keyMap[k1] = i

		k2 := opts.getItem(opts.rand.Intn(opts.NumItems))
		for _, ok := keyMap[k2]; ok; {
			k2 = opts.getItem(opts.rand.Intn(opts.NumItems))
		}
		keyMap[k2] = i

		keyPairs = append(keyPairs, [2]string{k1, k2})
	}

	return keyPairs, nil
}
