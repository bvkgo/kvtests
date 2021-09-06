package kvtests

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/bvkgo/kv"
)

type Options struct {
	NewTx kv.NewTxFunc
	NewIt kv.NewIterFunc

	Seed int64

	NumKeys int

	rand *rand.Rand
}

func (opts *Options) setDefaults() {
	if opts.NumKeys == 0 {
		opts.NumKeys = 1000
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

func (opts *Options) getKey(i int) string {
	return fmt.Sprintf("%08d", i)
}

func (opts *Options) selectKeys(n int) ([]string, error) {
	if opts.NumKeys < n*2 {
		return nil, fmt.Errorf("at least %d keys are required", 2*n)
	}

	var keys []string
	usedKeys := make(map[int]struct{})
	for i := 0; i < n; i++ {
		k := opts.rand.Intn(opts.NumKeys)
		for _, ok := usedKeys[k]; ok; {
			k = opts.rand.Intn(opts.NumKeys)
		}
		keys = append(keys, opts.getKey(k))
	}
	return keys, nil
}

// selectKeySets retrieves 'nset' slices of each with 'nkey' randomly chosen
// non-duplicate keys.
func (opts *Options) selectKeySets(nset, nkey int) ([][]string, error) {
	if opts.NumKeys < nset*nkey*2 {
		return nil, fmt.Errorf("at least %d keys are required", 2*nset*nkey)
	}

	rs := make([]int, 0, nset*nkey)
	usedKeys := make(map[int]struct{})
	for i := 0; i < nset*nkey; i++ {
		k := opts.rand.Intn(opts.NumKeys)
		for _, ok := usedKeys[k]; ok; {
			k = opts.rand.Intn(opts.NumKeys)
		}
		rs = append(rs, k)
	}

	start := 0
	keys := make([][]string, nset)
	for i := range keys {
		keys[i] = make([]string, 0, nkey)

		for j := start; j < len(rs); j++ {
			keys[i] = append(keys[i], opts.getKey(rs[j]))
		}
	}
	return keys, nil
}
