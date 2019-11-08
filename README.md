### Directed Acyclic Graph

#### Unopinionated, succinct, parallel runnable DAG lib for Go

````
t1 := NewTask("t1", func(c context.Context) error {
    fmt.Println("this is task 1")
    return nil
})
t2 := NewTask("t2", func(c context.Context) error {
    fmt.Println("this is task 2")
    return nil
})
f := NewFlow()
f.Add(t2).WithDeps(t1)
err := f.Run(context.Background())
if err != nil {
    panic(err)
}
````