package main

import (
	"log"
	"net/rpc"
)

func main(){
	client, err := rpc.DialHTTP("tcp","localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	var reply int
	client.Call("Arith.Multiply",{1,2},&reply)
}
