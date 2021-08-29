package kvtests

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bvkgo/kv"
)

func AbortedReads(ctx context.Context, opts *Options) error {
	steps := []string{
		"t1-begin",
		"t2-begin",
		"t1-k1-set-A",
		"t2-k1-get",
		"t1-abort",
		"t2-commit",
	}
	it, err := NewIsolationTest(steps)
	if err != nil {
		return err
	}

	opts.setDefaults()
	if err := opts.Check(); err != nil {
		return err
	}
	if _, _, err := FillItems(ctx, opts); err != nil {
		return err
	}
	if err := it.Run(ctx, opts, opts.getKeyPair()); err != nil {
		return err
	}
	// TODO: Check the result and intermediate states.
	log.Printf("Test: %#v", it.result)
	log.Printf("T1-T2: %#v", it.t1t2.result)
	log.Printf("T2-T1: %#v", it.t2t1.result)
	return nil
}

func RepeatedReads(ctx context.Context, opts *Options) error {
	opts.setDefaults()
	if err := opts.Check(); err != nil {
		return err
	}
	if _, _, err := FillItems(ctx, opts); err != nil {
		return err
	}

	steps := []string{
		"t1-begin",
		"t2-begin",
		"t2-k1-get",
		"t1-k1-set-A",
		"t1-commit",
		"t2-k1-get",
		"t2-commit",
	}

	it, err := NewIsolationTest(steps)
	if err != nil {
		return err
	}
	// T2 is allowed to fail due to read-write conflict.
	if err := it.Run(ctx, opts, opts.getKeyPair()); err != nil {
		if e, ok := err.(*ErrCommitFailure); !ok || e.Tx != "t2" {
			return err
		}
	}
	t2get1, err := it.GetResultAt(2)
	if err != nil {
		return err
	}
	t2get2, err := it.GetResultAt(5)
	if err != nil {
		return err
	}
	if t2get1 != t2get2 {
		return fmt.Errorf("noticed read-committed")
	}
	// TODO Check the result and intermediate states.
	log.Printf("Test: %#v", it.result)
	log.Printf("T1-T2: %#v", it.t1t2.result)
	log.Printf("T2-T1: %#v", it.t2t1.result)
	return nil
}

type ErrCommitFailure struct {
	Tx   string
	Orig error
}

func (e *ErrCommitFailure) Error() string {
	return fmt.Sprintf("tx %s commit failed: %v", e.Tx, e.Orig)
}

type IsolationTest struct {
	steps []string

	result []string

	// gets holds the history of results for the get operation as a mapping from
	// step index to the result.
	gets map[int]string

	t1t2 *IsolationTest
	t2t1 *IsolationTest
}

func NewIsolationTest(steps []string) (*IsolationTest, error) {
	if err := checkSteps(steps); err != nil {
		return nil, err
	}
	t1t2steps, t2t1steps := serializeSteps(steps)
	it := &IsolationTest{
		steps: steps,
		gets:  make(map[int]string),
		t1t2: &IsolationTest{
			steps: t1t2steps,
			gets:  make(map[int]string),
		},
		t2t1: &IsolationTest{
			steps: t2t1steps,
			gets:  make(map[int]string),
		},
	}
	return it, nil
}

func (it *IsolationTest) GetResultAt(index int) (string, error) {
	v, ok := it.gets[index]
	if !ok {
		return "", os.ErrInvalid
	}
	return v, nil
}

func (it *IsolationTest) Run(ctx context.Context, opts *Options, keys [2]string) error {
	if err := it.runSteps(ctx, opts, keys, it.steps); err != nil {
		return err
	}
	result, err := getKeys(ctx, opts, keys[:])
	if err != nil {
		return err
	}
	it.result = result

	if it.t1t2 != nil {
		if err := it.t1t2.runSteps(ctx, opts, keys, it.t1t2.steps); err != nil {
			return err
		}
		result, err := getKeys(ctx, opts, keys[:])
		if err != nil {
			return err
		}
		it.t1t2.result = result
	}
	if it.t2t1 != nil {
		if err := it.t2t1.runSteps(ctx, opts, keys, it.t2t1.steps); err != nil {
			return err
		}
		result, err := getKeys(ctx, opts, keys[:])
		if err != nil {
			return err
		}
		it.t2t1.result = result
	}
	return nil
}

func (it *IsolationTest) runSteps(ctx context.Context, opts *Options, keys [2]string, steps []string) (status error) {
	if err := checkSteps(steps); err != nil {
		return err
	}

	if err := clearKeys(ctx, opts, keys[:]); err != nil {
		return err
	}

	var txes [2]kv.Transaction
	defer func() {
		if status != nil {
			if txes[0] != nil {
				_ = txes[0].Rollback(ctx)
			}
			if txes[1] != nil {
				_ = txes[1].Rollback(ctx)
			}
		}
	}()

	for index, step := range steps {
		i := -1
		if strings.Contains(step, "t1") {
			i = 0
		} else if strings.Contains(step, "t2") {
			i = 1
		} else {
			return os.ErrInvalid
		}

		j := -1
		if strings.Contains(step, "get") || strings.Contains(step, "set") || strings.Contains(step, "delete") {
			if strings.Contains(step, "k1") {
				j = 0
			} else if strings.Contains(step, "k2") {
				j = 1
			} else {
				return os.ErrInvalid
			}
		}

		newvalue := ""
		if strings.Contains(step, "set") {
			// eg: get 10 from t1-k1-set-10.
			fs := strings.Split(step, "-")
			newvalue = fs[len(fs)-1]
		}

		// Take the appropriate action.

		if strings.Contains(step, "begin") {
			tx, err := opts.NewTx(ctx)
			if err != nil {
				return err
			}
			txes[i] = tx
			continue
		}

		if strings.Contains(step, "get") {
			v, err := txes[i].Get(ctx, keys[j])
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return err
				}
				v = "os.ErrNotExist"
			}
			// Save to the gets history.
			it.gets[index] = v
			continue
		}

		if strings.Contains(step, "set") {
			if err := txes[i].Set(ctx, keys[j], newvalue); err != nil {
				return err
			}
			continue
		}

		if strings.Contains(step, "delete") {
			if err := txes[i].Delete(ctx, keys[j]); err != nil {
				return err
			}
			continue
		}

		if strings.Contains(step, "abort") {
			if err := txes[i].Rollback(ctx); err != nil {
				return err
			}
			continue
		}

		if strings.Contains(step, "commit") {
			if err := txes[i].Commit(ctx); err != nil {
				if strings.Contains(step, "t1") {
					return &ErrCommitFailure{Tx: "t1", Orig: err}
				} else {
					return &ErrCommitFailure{Tx: "t2", Orig: err}
				}
			}
			continue
		}
	}
	return nil
}

// NOTE: two txes only
func serializeSteps(steps []string) ([]string, []string) {
	var t1steps []string
	var t2steps []string
	for _, step := range steps {
		if strings.Contains(step, "t1") {
			t1steps = append(t1steps, step)
		} else if strings.Contains(step, "t2") {
			t2steps = append(t2steps, step)
		} else {
			panic("os.ErrInvalid")
		}
	}
	return append(t1steps, t2steps...), append(t2steps, t1steps...)
}

func checkSteps(steps []string) error {
	// 1. Every step must include t1 or t2, but not both.
	// 2. There can only one of get|set|delete|commit|abort in a step.
	// 3. get|set|delete must have one of k1 or k2, but not both.
	// 4. set must have a new-value at the end.
	// 5. both tx must have a commit|abort step, but not both and only once.
	return nil
}

func clearKeys(ctx context.Context, opts *Options, keys []string) (status error) {
	tx, err := opts.NewTx(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if status != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	for _, k := range keys {
		if err := tx.Set(ctx, k, k); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func getKeys(ctx context.Context, opts *Options, keys []string) (vs []string, status error) {
	tx, err := opts.NewTx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if status != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	for _, k := range keys {
		v, err := tx.Get(ctx, k)
		if err != nil {
			return nil, err
		}
		vs = append(vs, v)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return vs, nil
}
