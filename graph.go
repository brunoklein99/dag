package dag

import (
	"context"
	"sync"
)

type nodeFunc = func(context.Context) error

type graph struct {
	roots []*node
	mu    sync.Mutex
}

func newGraph(roots []*node) *graph {
	return &graph{roots: roots}
}

type node struct {
	name  string
	adjs  []*node
	ndeps int
	fn    nodeFunc
}

func newNode(name string, fn nodeFunc) *node {
	return &node{name: name, fn: fn}
}

func (g *graph) runNode(n *node, ctx context.Context, wg *sync.WaitGroup, ch chan<- error) {
	defer wg.Done()
	err := n.fn(ctx)
	if err != nil {
		ch <- err
		return
	}
	for _, node := range n.adjs {
		select {
		case <-ctx.Done():
			break
		default:
			adj := node
			dec := func() {
				g.mu.Lock()
				defer g.mu.Unlock()
				adj.ndeps--
			}
			dec()
			if adj.ndeps == 0 {
				wg.Add(1)
				go g.runNode(adj, ctx, wg, ch)
			}
		}
	}
}

func (g *graph) run(ctx context.Context) error {
	chError := make(chan error)
	chDone := make(chan bool)
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, node := range g.roots {
		root := node
		wg.Add(1)
		go g.runNode(root, ctx, &wg, chError)
	}
	go func() {
		wg.Wait()
		chDone <- true
	}()
	var err error
	select {
	case err = <-chError:
		cancel()
	case <-chDone:
		break
	}
	return err
}
