package main

import "fmt"

type Test struct {
	A interface{}
}

func main() {
	t := Test{}
	t.A = 1

	fmt.Println(t.A)
}
