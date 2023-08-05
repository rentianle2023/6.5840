package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := Task{}
		ok := call("Coordinator.AssignTask", &ExampleArgs{}, &task)
		if !ok {
			break
		}

		switch task.Status {
		case Map:
			performMapTask(&task, mapf)
			break
		case Reduce:
			performReduceTask(task, reducef)
			call("Coordinator.TaskComplete", &task, &ExampleReply{})
			break
		case Wait:
			time.Sleep(5 * time.Second)
			break
		case Exit:
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func performReduceTask(task Task, reducef func(string, []string) string) {
	filenames := task.Input
	var intermediate []KeyValue
	for _, filename := range filenames {
		kv := readFromFile(filename)
		intermediate = append(intermediate, kv...)
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(task.TaskId)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}

func performMapTask(task *Task, mapf func(string, string) []KeyValue) {
	filename := task.Input[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % task.NReduce
		intermediate[index] = append(intermediate[index], kv)
	}
	for k, v := range intermediate {
		task.Intermediate[k] = writeToFile(filename+strconv.Itoa(k), v)
	}
	call("Coordinator.TaskComplete", &Task{TaskId: task.TaskId, Intermediate: task.Intermediate}, &ExampleReply{})
}

func writeToFile(filename string, keyValues []KeyValue) string {
	file, err := os.CreateTemp("/Users/cencen/Desktop/6.5840/src/main/tmp", "whatEver")
	if err != nil {
		fmt.Println(err)
	}
	enc := json.NewEncoder(file)
	for _, kv := range keyValues {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("error : ", err, file.Name())
		}
	}
	return file.Name()
}

func readFromFile(filename string) []KeyValue {
	file, _ := os.OpenFile(filename, os.O_RDONLY, os.ModeAppend)
	var kva []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
