package mr

import "C"
import (
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type status uint8

const (
	LEISURE status = iota
	BUSY
	COMPLETED
)

const (
	MAPPERIOD = iota
	REDUCEPERIOD
	COMPLETEDPERIOD
)

type mapTask struct {
	Status       status    `json:"status"`
	TimeDelay    time.Time `json:"timeDelay"`
	Intermediate []string  `json:"intermediate"`
}

type reduceTask struct {
	Status status `json:"status"`
}

type Coordinator struct {
	rw          sync.RWMutex
	NReduce     int          `json:"NReduce"`
	NMap        int          `json:"NMap"`
	Period      int          `json:"period"`
	Files       []string     `json:"files"`
	MapTasks    []mapTask    `json:"tasks"`
	ReduceTasks []reduceTask `json:"reduceTask"`
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
	ret := false

	// Your code here.
	c.rw.RLock()
	defer c.rw.RUnlock()
	if c.Period == COMPLETEDPERIOD {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{MapTasks: make([]mapTask, len(files)), ReduceTasks: make([]reduceTask, nReduce)}

	c.NReduce = nReduce
	c.NMap = len(files)
	c.Files = files

	c.server()
	return &c
}

func goSafe(f func()) {
	defer func() {
		if p := recover(); p != nil {
			log.Println(p)
		}
	}()
	f()
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.rw.Lock()
	defer c.rw.Unlock()

	id := math.MaxInt

	// map阶段
	if c.Period == MAPPERIOD {
		for i, task := range c.MapTasks {
			if task.Status == LEISURE {
				id = i
				break
			}
		}
		if id != math.MaxInt {
			reply.Assigned = true
			reply.TaskID = id
			reply.NReduce = c.NReduce
			reply.Category = MAPPER
			reply.MapFile = c.Files[id]

			c.MapTasks[id].Status = BUSY
			go goSafe(func() {
				time.Sleep(10 * time.Second)
				c.rw.Lock()
				defer c.rw.Unlock()
				if c.MapTasks[id].Status != COMPLETED {
					c.MapTasks[id].Status = LEISURE
				}
			})
		}
		return nil
	}

	// reduce阶段
	for i, task := range c.ReduceTasks {
		if task.Status == LEISURE {
			id = i
			break
		}
	}
	if id == math.MaxInt {
		return nil
	}
	reply.Assigned = true
	reply.TaskID = id
	reply.NReduce = c.NReduce
	reply.Category = REDUCER
	reply.ReduceFiles = []string{}
	c.ReduceTasks[id].Status = BUSY
	for i := 0; i < c.NMap; i++ {
		reply.ReduceFiles = append(reply.ReduceFiles, c.MapTasks[i].Intermediate[id])
	}
	go goSafe(func() {
		time.Sleep(10 * time.Second)
		c.rw.Lock()
		defer c.rw.Unlock()
		if c.ReduceTasks[id].Status != COMPLETED {
			c.ReduceTasks[id].Status = LEISURE
		}
	})

	return nil
}

func (c *Coordinator) CommitMap(args *CommitMapArgs, reply *CommitMapReply) error {
	c.rw.Lock()
	defer c.rw.Unlock()
	c.MapTasks[args.TaskID].Status = COMPLETED
	c.MapTasks[args.TaskID].Intermediate = args.Files
	for _, task := range c.MapTasks {
		if task.Status != COMPLETED {
			return nil
		}
	}
	c.Period = REDUCEPERIOD
	return nil
}

func (c *Coordinator) CommitReduce(args *CommitReduceArgs, reply *CommitReduceReply) error {
	c.rw.Lock()
	defer c.rw.Unlock()
	c.ReduceTasks[args.TaskID].Status = COMPLETED
	for _, task := range c.ReduceTasks {
		if task.Status != COMPLETED {
			return nil
		}
	}
	c.Period = COMPLETEDPERIOD
	return nil
}
