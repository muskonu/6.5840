package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

// Add your RPC definitions here.
type category string

const (
	MAPPER  category = "mapper"
	REDUCER category = "reducer"
)

type RegisterArgs struct {
}

type RegisterReply struct {
	Assigned    bool     `json:"assigned"`
	TaskID      int      `json:"taskID"`
	NReduce     int      `json:"NReduce"`
	Category    category `json:"category"`
	MapFile     string   `json:"mapFile"`
	ReduceFiles []string `json:"reduceFiles"`
}

type CommitMapArgs struct {
	TaskID int      `json:"taskID"`
	Files  []string `json:"files"`
}

type CommitMapReply struct {
}

type CommitReduceArgs struct {
	TaskID int `json:"taskID"`
}

type CommitReduceReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
