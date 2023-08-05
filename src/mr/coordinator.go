package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorStatus int

const (
	Map CoordinatorStatus = iota
	Reduce
	Exit
	Wait
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Complete
)

type Coordinator struct {
	// Your definitions here.
	TaskQueue    chan Task
	TaskMeta     []TaskMeta
	Intermediate map[int][]string // reduceId对应的list of files
	Status       CoordinatorStatus
	NReduce      int
	Mu           sync.Mutex
}

type Task struct {
	TaskId       int
	Intermediate map[int]string
	Status       CoordinatorStatus
	Input        []string
	NReduce      int
}

type TaskMeta struct {
	TaskId    int
	StartTime time.Time
	Status    TaskStatus
	Input     []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if len(c.TaskQueue) > 0 {
		*reply = <-c.TaskQueue
		c.TaskMeta[reply.TaskId].StartTime = time.Now()
		c.TaskMeta[reply.TaskId].Status = InProgress
	} else if c.Status == Exit {
		*reply = Task{Status: Exit}
	} else {
		*reply = Task{Status: Wait}
	}
	return nil
}

func (c *Coordinator) checkAllTaskComplete() {
	for _, meta := range c.TaskMeta {
		if meta.Status != Complete {
			return
		}
	}
	if c.Status == Map {
		c.initReduceTasks()
	} else {
		for _, files := range c.Intermediate {
			for _, file := range files {
				os.Remove(file)
			}
		}
		c.Status = Exit
	}
}

func (c *Coordinator) initReduceTasks() {
	c.Status = Reduce
	c.TaskMeta = make([]TaskMeta, c.NReduce)
	c.TaskQueue = make(chan Task, c.NReduce)
	for i, intermediate := range c.Intermediate {
		task := Task{
			TaskId:  i,
			Input:   intermediate,
			Status:  Reduce,
			NReduce: c.NReduce}
		taskMeta := TaskMeta{
			TaskId: i,
			Status: Idle,
			Input:  intermediate,
		}
		c.TaskQueue <- task
		c.TaskMeta[i] = taskMeta
	}
}

func (c *Coordinator) TaskComplete(task *Task, reply *ExampleReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	c.TaskMeta[task.TaskId].Status = Complete
	for i, intermediate := range task.Intermediate {
		c.Intermediate[i] = append(c.Intermediate[i], intermediate)
	}
	c.checkAllTaskComplete()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.Status == Exit
}

func (c *Coordinator) checkCrash() {
	for {
		time.Sleep(time.Second * 5)
		c.Mu.Lock()
		for _, taskMeta := range c.TaskMeta {
			if taskMeta.Status == InProgress && time.Now().Sub(taskMeta.StartTime) > time.Second*10 {
				taskMeta.Status = Idle
				c.TaskQueue <- Task{
					TaskId:       taskMeta.TaskId,
					Input:        taskMeta.Input,
					Status:       c.Status,
					NReduce:      c.NReduce,
					Intermediate: make(map[int]string),
				}
			}
		}
		c.Mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:    make(chan Task, len(files)),
		TaskMeta:     make([]TaskMeta, len(files)),
		Intermediate: make(map[int][]string),
		Status:       Map,
		NReduce:      nReduce,
		Mu:           sync.Mutex{},
	}

	for i, file := range files {
		task := Task{
			Status:       Map,
			TaskId:       i,
			Input:        []string{file},
			NReduce:      nReduce,
			Intermediate: make(map[int]string),
		}
		taskMeta := TaskMeta{
			Status: Idle,
			TaskId: i,
			Input:  []string{file},
		}
		c.TaskQueue <- task
		c.TaskMeta[i] = taskMeta
	}
	go c.checkCrash()
	c.server()

	return &c
}
