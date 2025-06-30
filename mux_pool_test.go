package connpool

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type testCase struct {
	Int int64
}

func TestMuxPool(t *testing.T) {
	var st1 = &testCase{}
	var st2 = &testCase{}
	fmt.Println(st1 == st2)
	fmt.Printf("%p\n", st1)
	fmt.Printf("%p\n", st2)

	muxPool, err := NewMuxPool(2, 200, func() (interface{}, error) {
		return &testCase{}, nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	var (
		//wg sync.WaitGroup
		c   atomic.Int32
		all []interface{}
	)
	//wg.Add(5000)
	for i := 0; i < 10; i++ {
		conn, isNew, err := muxPool.Get()
		if err != nil {
			t.Fatal(err)
			return
		}
		if i < 5 {
			muxPool.Block(conn)
			all = append(all, isNew)
		}
		if isNew {
			c.Add(1)
		}
	}

	for _, conn := range all {
		muxPool.Put(conn)
	}

	fmt.Println(c.Load())
	fmt.Println(muxPool.Len())

	fmt.Println("========")

	c.Store(0)
	for i := 0; i < 10; i++ {
		_, isNew, err := muxPool.Get()
		if err != nil {
			t.Fatal(err)
			return
		}
		if isNew {
			c.Add(1)
		}
	}

	//wg.Wait()

	fmt.Println(c.Load())
	fmt.Println(muxPool.Len())

	muxPool.RegisterChecker(time.Millisecond, func(payload interface{}) bool {
		fmt.Println("checker")
		return false
	})

	time.Sleep(time.Second)

	fmt.Println("@@@@@@")

	fmt.Println(muxPool.Len())

	fmt.Println("========")

	muxPool.Destroy()

	fmt.Println("========")

	fmt.Println(muxPool.Len())

	muxPool.Clear()

	fmt.Println(muxPool.Len())
}
