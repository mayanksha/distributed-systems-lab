package main

import (
	"fmt"
	"golang.org/x/tour/tree"
	"sync"
)

// Walk walks the tree t sending all values
// from the tree to the channel ch.
func Walk(t *tree.Tree, ch chan int) {
	if (t == nil) {
		return
	}

	Walk(t.Left, ch)
	ch <- t.Value
	Walk(t.Right, ch)
}

func WalkHelper(t *tree.Tree, ch chan int, wg *sync.WaitGroup) {
	wg.Add(1)
	Walk(t, ch)
	close(ch)
	wg.Done()
}

// Same determines whether the trees
// t1 and t2 contain the same values.
func Same(t1, t2 *tree.Tree) bool {
	ch1, ch2 := make(chan int), make(chan int)
	var wg sync.WaitGroup

	go WalkHelper(t1, ch1, &wg)
	go WalkHelper(t2, ch2, &wg)

	for v := range ch1 {
		k, ok := <- ch2
		if (!ok || k != v) {
			return false
		}
	}
	wg.Wait()

	return true
}

func mmmain() {

	fmt.Println(Same(tree.New(1), tree.New(1)))
	fmt.Println(Same(tree.New(1), tree.New(2)))
}
