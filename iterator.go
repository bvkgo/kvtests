package kvtests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/bvkgo/kv"
)

type IteratorData struct {
	count int

	f, l int

	first, last string
}

func (id *IteratorData) HandleAscend(ctx context.Context, it kv.Iterator) error {
	var err error
	for k, _, err := it.GetNext(ctx); err == nil; k, _, err = it.GetNext(ctx) {
		if len(k) == 0 {
			return fmt.Errorf("key cannot be empty")
		}
		v, err := strconv.Atoi(k)
		if err != nil {
			return fmt.Errorf("could not parse key to int: %w", err)
		}
		if id.count > 0 {
			if v < id.l {
				return fmt.Errorf("last key %d was larger than current %d", id.l, v)
			}
			if v == id.l {
				return fmt.Errorf("last key %d was same as the current %d", id.l, v)
			}
			if v != id.l+1 {
				return fmt.Errorf("wanted %d, got %d", id.l+1, v)
			}
		}
		if id.count == 0 {
			id.f, id.first = v, k
		}
		id.count++
		id.l, id.last = v, k
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (id *IteratorData) HandleDescend(ctx context.Context, it kv.Iterator) error {
	var err error
	for k, _, err := it.GetNext(ctx); err == nil; k, _, err = it.GetNext(ctx) {
		if len(k) == 0 {
			return fmt.Errorf("key cannot be empty")
		}
		v, err := strconv.Atoi(k)
		if err != nil {
			return fmt.Errorf("could not parse key to int: %w", err)
		}
		if id.count > 0 {
			if v > id.l {
				return fmt.Errorf("last key %d was smaller than current %d", id.l, v)
			}
			if v == id.l {
				return fmt.Errorf("last key %d was same as the current %d", id.l, v)
			}
			if v != id.l-1 {
				return fmt.Errorf("wanted %d, got %d", id.l-1, v)
			}
		}
		if id.count == 0 {
			id.f, id.first = v, k
		}
		id.count++
		id.l, id.last = v, k
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func RunAscendTest1(ctx context.Context, opts *Options) error {
	opts.setDefaults()
	if err := opts.Check(); err != nil {
		return err
	}

	nkeys := opts.NumKeys
	if nkeys < 1000 {
		return fmt.Errorf("this test needs minimum 1000 keys: %w", os.ErrInvalid)
	}

	_, largest, err := FillItems(ctx, opts)
	if err != nil {
		return err
	}

	// Iterate all keys in ascending order.
	{
		tx, err := opts.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := opts.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Ascend(ctx, "", "", it); err != nil {
			return err
		}
		var id IteratorData
		if err := id.HandleAscend(ctx, it); err != nil {
			return err
		}
		if err := tx.Discard(ctx); err != nil {
			return err
		}
		if id.count != nkeys {
			return fmt.Errorf("wanted %d callbacks, got %d (first %s last %s)", nkeys, id.count, id.first, id.last)
		}
	}

	// Iterate till the largest key with one of i or j as the empty string.
	{
		r := opts.rand.Intn(nkeys)
		x := opts.getKey(r)

		tx, err := opts.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := opts.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Ascend(ctx, x, "", it); err != nil {
			return err
		}
		var id IteratorData
		if err := id.HandleAscend(ctx, it); err != nil {
			return err
		}
		if err := tx.Discard(ctx); err != nil {
			return err
		}
		if id.first != x {
			return fmt.Errorf("wanted %s as the first, got %s", x, id.first)
		}
		if id.last != largest {
			return fmt.Errorf("wanted %s as the last, got %s", largest, id.last)
		}
		if id.count != nkeys-r {
			return fmt.Errorf("wanted %d callbacks, got %d", nkeys-r, id.count)
		}
	}
	{
		r := opts.rand.Intn(nkeys)
		x := opts.getKey(r)

		tx, err := opts.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := opts.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Ascend(ctx, "", x, it); err != nil {
			return err
		}
		var id IteratorData
		if err := id.HandleAscend(ctx, it); err != nil {
			return err
		}
		if err := tx.Discard(ctx); err != nil {
			return err
		}
		if id.first != x {
			return fmt.Errorf("wanted %s as the first, got %s", x, id.first)
		}
		if id.last != largest {
			return fmt.Errorf("wanted %s as the last, got %s", largest, id.last)
		}
		if id.count != nkeys-r {
			return fmt.Errorf("wanted %d callbacks, got %d", nkeys-r, id.count)
		}
	}

	// Iterate randomly picked range.
	{
		b := opts.rand.Intn(nkeys)
		e := opts.rand.Intn(nkeys)
		x := opts.getKey(b)
		y := opts.getKey(e)
		min, max, count := x, opts.getKey(e-1), e-b
		if y < x {
			min, max, count = y, opts.getKey(b-1), b-e
		}

		tx, err := opts.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := opts.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Ascend(ctx, x, y, it); err != nil {
			return err
		}
		var id IteratorData
		if err := id.HandleAscend(ctx, it); err != nil {
			return err
		}
		if err := tx.Discard(ctx); err != nil {
			return err
		}
		if id.count != count {
			return fmt.Errorf("wanted %d callbacks, got %d", count, id.count)
		}
		if count > 0 {
			if id.first != min {
				return fmt.Errorf("wanted %s as the first, got %s", min, id.first)
			}
			if id.last != max {
				return fmt.Errorf("wanted %s as the last, got %s", max, id.last)
			}
		}
	}

	return nil
}

func RunDescendTest1(ctx context.Context, opts *Options) error {
	opts.setDefaults()
	if err := opts.Check(); err != nil {
		return err
	}

	nkeys := opts.NumKeys
	if nkeys < 1000 {
		return fmt.Errorf("this test needs minimum 1000 keys: %w", os.ErrInvalid)
	}

	smallest, largest, err := FillItems(ctx, opts)
	if err != nil {
		return err
	}

	// Iterate all keys in ascending order.
	{

		tx, err := opts.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := opts.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Descend(ctx, "", "", it); err != nil {
			return err
		}
		var id IteratorData
		if err := id.HandleDescend(ctx, it); err != nil {
			return err
		}
		if err := tx.Discard(ctx); err != nil {
			return err
		}
		if id.count != nkeys {
			return fmt.Errorf("wanted %d callbacks, got %d", nkeys, id.count)
		}
		if id.first != largest {
			return fmt.Errorf("wanted %q as the first, got %q", largest, id.first)
		}
		if id.last != smallest {
			return fmt.Errorf("wanted %q as the last, got %q", smallest, id.last)
		}
	}

	// Iterate till the smallest key with one of i or j as the empty string.
	{
		r := opts.rand.Intn(nkeys)
		x := opts.getKey(r)

		tx, err := opts.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := opts.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Descend(ctx, x, "", it); err != nil {
			return err
		}
		var id IteratorData
		if err := id.HandleDescend(ctx, it); err != nil {
			return err
		}
		if err := tx.Discard(ctx); err != nil {
			return err
		}
		if id.first != x {
			return fmt.Errorf("wanted %s as the first, got %s", x, id.first)
		}
		if id.last != smallest {
			return fmt.Errorf("wanted %s as the last, got %s", smallest, id.last)
		}
		if id.count != r+1 {
			return fmt.Errorf("wanted %d callbacks, got %d", r+1, id.count)
		}
	}
	{
		r := opts.rand.Intn(nkeys)
		x := opts.getKey(r)

		tx, err := opts.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := opts.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Descend(ctx, "", x, it); err != nil {
			return err
		}
		var id IteratorData
		if err := id.HandleDescend(ctx, it); err != nil {
			return err
		}
		if err := tx.Discard(ctx); err != nil {
			return err
		}
		if id.first != x {
			return fmt.Errorf("wanted %s as the first, got %s", x, id.first)
		}
		if id.last != smallest {
			return fmt.Errorf("wanted %s as the last, got %s", smallest, id.last)
		}
		if id.count != r+1 {
			return fmt.Errorf("wanted %d callbacks, got %d", r+1, id.count)
		}
	}

	// Iterate randomly picked range.
	{
		f := opts.rand.Intn(nkeys)
		l := opts.rand.Intn(nkeys)
		if f < l {
			f, l = l, f
		}
		x := opts.getKey(f)
		y := opts.getKey(l)
		min, max, count := opts.getKey(l+1), x, f-l

		tx, err := opts.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := opts.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Descend(ctx, x, y, it); err != nil {
			return err
		}
		var id IteratorData
		if err := id.HandleDescend(ctx, it); err != nil {
			return err
		}
		if err := tx.Discard(ctx); err != nil {
			return err
		}
		if id.count != count {
			return fmt.Errorf("wanted %d callbacks, got %d", count, id.count)
		}
		if count > 0 {
			if id.first != max {
				return fmt.Errorf("wanted %s as the first, got %s", max, id.first)
			}
			if id.last != min {
				return fmt.Errorf("wanted %s as the last, got %s", min, id.last)
			}
		}
	}

	return nil
}
