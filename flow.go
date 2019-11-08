package dag

import (
	"context"
	"fmt"
)

type Flow struct {
	tasks []*Task
}

func NewFlow() *Flow {
	return &Flow{}
}

func (f *Flow) Add(t *Task) *Task {
	if t.id > 0 {
		return f.tasks[t.id-1]
	}

	t.id = len(f.tasks) + 1
	t.f = f

	f.tasks = append(f.tasks, t)
	return t
}

func (f *Flow) compile() (*graph, error) {
	var roots []*node
	nodes := make([]*node, len(f.tasks))
	for i := 0; i < len(nodes); i++ {
		nodes[i] = &node{}
		nodes[i].adjs = []*node{}
	}
	for i, task := range f.tasks {
		nodes[i].name = task.name
		nodes[i].fn = task.fn
		nodes[i].ndeps = len(task.deps)
		//assign current as adj to parents
		for _, dep := range task.deps {
			p := nodes[dep.id-1]
			p.adjs = append(p.adjs, nodes[i])
		}
		if len(task.deps) == 0 {
			roots = append(roots, nodes[i])
		}
	}
	return newGraph(roots), nil
}

func (f *Flow) Run(ctx context.Context) error {
	g, err := f.compile()
	if err != nil {
		return fmt.Errorf("could not run flow: %w", err)
	}
	return g.run(ctx)
}

type TaskFunc = func(context.Context) error

type Task struct {
	id   int
	name string
	deps []*Task
	fn   TaskFunc
	f    *Flow
}

func NewTask(name string, fn TaskFunc) *Task {
	return &Task{name: name, fn: fn}
}

func (t *Task) WithDeps(tasks ...*Task) {
	//adds deps that are not added in flow
	for _, task := range tasks {
		if task.id == 0 {
			t.f.Add(task)
		}
	}
	t.deps = append(t.deps, tasks...)
}

func (t *Task) Name() string {
	return t.name
}
