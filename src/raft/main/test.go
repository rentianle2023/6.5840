package main

import (
	"time"
)

type Test struct {
	A interface{}
}

func main() {
	t := time.NewTicker(time.Second * 5)
}
