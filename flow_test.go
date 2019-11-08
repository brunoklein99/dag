package dag

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func taskFactory(name string, fn TaskFunc) *Task {
	return NewTask(name, func(c context.Context) error {
		fmt.Printf("starting %s\n", name)
		err := fn(c)
		fmt.Printf("finishing %s\n", name)
		return err
	})
}

func TestOneAfterAnother(t *testing.T)  {
	var s []int

	t1 := taskFactory("t1", func(c context.Context) error {
		time.Sleep(time.Second * 1)
		s = append(s, 1)
		return nil
	})
	t2 := taskFactory("t2", func(c context.Context) error {
		s = append(s, 2)
		return nil
	})

	f := NewFlow()
	f.Add(t2).WithDeps(t1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := f.Run(context.Background())
		if err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()

	assert.Equal(t, []int{1, 2}, s)
}

func TestTwoAfterOne(t *testing.T)  {
	var s []int

	t1 := taskFactory("t1", func(c context.Context) error {
		s = append(s, 1)
		return nil
	})
	t2 := taskFactory("t2", func(c context.Context) error {
		s = append(s, 2)
		return nil
	})
	t3 := taskFactory("t3", func(c context.Context) error {
		s = append(s, 3)
		return nil
	})

	f := NewFlow()
	f.Add(t3).WithDeps(t1)
	f.Add(t2).WithDeps(t1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := f.Run(context.Background())
		if err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()

	assert.Len(t, s, 3)
	assert.Equal(t, s[0], 1)
}

func TestOneWithTwoDeps(t *testing.T)  {
	var s []int

	t1 := taskFactory("t1", func(c context.Context) error {
		time.Sleep(time.Second * 1)
		s = append(s, 1)
		return nil
	})
	t2 := taskFactory("t2", func(c context.Context) error {
		time.Sleep(time.Second * 1)
		s = append(s, 2)
		return nil
	})
	t3 := taskFactory("t3", func(c context.Context) error {
		s = append(s, 3)
		return nil
	})

	f := NewFlow()
	f.Add(t3).WithDeps(t1, t2)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := f.Run(context.Background())
		if err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()

	assert.Len(t, s, 3)
	assert.Equal(t, s[2], 3)
}

func TestWhenErrorNotLaunchRemaining(t *testing.T)  {
	var s []int

	t1 := taskFactory("t1", func(c context.Context) error {
		s = append(s, 1)
		return errors.New("this is error")
	})
	t2 := taskFactory("t2", func(c context.Context) error {
		s = append(s, 2)
		return errors.New("this is error")
	})
	t3 := taskFactory("t3", func(c context.Context) error {
		s = append(s, 3)
		return nil
	})

	f := NewFlow()
	f.Add(t3).WithDeps(t1, t2)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.Error(t, f.Run(context.Background()))
	}()
	wg.Wait()

	assert.True(t, len(s) == 1 || len(s) == 2)
}