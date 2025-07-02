package connpool

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testCase struct {
	Int int64
}

func TestSlice(t *testing.T) {
	s := [10]*testCase{}
	var wg sync.WaitGroup
	wg.Add(1000000)
	for i := 0; i < 1000000; i++ {
		go func() {
			defer wg.Done()
			for j, ori := range s {
				if ori == nil {
				}

				s[j] = &testCase{
					Int: time.Now().Unix(),
				}
				//fmt.Println(ori)
			}
		}()
	}
	wg.Wait()
	fmt.Println(s)
}

var id atomic.Int64

func TestMuxPool(t *testing.T) {
	var st1 = &testCase{}
	var st2 = &testCase{}
	fmt.Println(st1 == st2)
	fmt.Printf("%p\n", st1)
	fmt.Printf("%p\n", st2)

	muxPool, err := NewMuxPool(2, 200, func() (interface{}, error) {
		id.Add(1)
		return &testCase{
			Int: id.Load(),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	muxPool.Ping = func(i interface{}) bool {
		return i.(*testCase).Int != 0
	}

	muxPool.Close = func(i interface{}) {
		i.(*testCase).Int = -1
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
		}
		all = append(all, conn)
		if isNew {
			c.Add(1)
		}
		fmt.Printf("get conn %d\n", conn.(*testCase).Int)
	}

	for i, conn := range all {
		if i < 3 {
			conn.(*testCase).Int = 0
		}
		muxPool.Put(conn)
	}

	fmt.Println(c.Load())
	fmt.Println(muxPool.Len())

	fmt.Println("========")

	c.Store(0)
	for i := 0; i < 10; i++ {
		cc, isNew, err := muxPool.Get()
		if err != nil {
			t.Fatal(err)
			return
		}
		if isNew {
			c.Add(1)
		}
		fmt.Printf("get conn2 %d\n", cc.(*testCase).Int)
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

func TestMuxPool3(t *testing.T) {
	muxPool, err := NewMuxPool(0, 10, func() (interface{}, error) {
		id.Add(1)
		return &testCase{
			Int: id.Load(),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	var all []interface{}
	var c atomic.Int32
	for i := 0; i < 10; i++ {
		conn, isNew, err := muxPool.Get()
		if err != nil {
			t.Fatal(err)
			return
		}
		muxPool.Block(conn)
		all = append(all, conn)
		if isNew {
			c.Add(1)
		}
		fmt.Printf("get conn %d\n", conn.(*testCase).Int)
	}

	fmt.Println(all)
	fmt.Println(muxPool.Len())

	for _, conn := range all {
		muxPool.Put(conn)
	}

	muxPool.RegisterChecker(time.Millisecond, func(payload interface{}) bool {
		fmt.Println("checker")
		return false
	})

	time.Sleep(time.Second)

	fmt.Println(muxPool.Len())
	fmt.Println(muxPool.store)

	muxPool.Get()
	fmt.Println(muxPool.Len())
	fmt.Println(muxPool.store)
}

func TestMuxPool2(t *testing.T) {
	muxPool, err := NewMuxPool(2, 10, func() (interface{}, error) {
		id.Add(1)
		return &testCase{
			Int: id.Load(),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			c, _, _ := muxPool.Get()
			rand.Seed(time.Now().UnixNano())
			r := rand.Intn(100)
			if r < 60 {
				muxPool.Block(c)
			}
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			muxPool.Put(c)
		}()
	}
	wg.Wait()
	fmt.Println(muxPool.Len())

	for _, conn := range muxPool.store {
		if conn == nil {
			continue
		}
		if conn.isBlocking {
			fmt.Println("blocking")
		}
	}
}
