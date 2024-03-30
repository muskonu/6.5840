package mr

import (
	"fmt"
	"io"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		args := RegisterArgs{}
		reply := RegisterReply{}
		if ok := call("Coordinator.Register", &args, &reply); !ok {
			break
		}
		if !reply.Assigned {
			time.Sleep(1 * time.Second)
			continue
		}
		switch reply.Category {
		case MAPPER:
			if ok := mapProcess(mapf, reply); !ok {
				return
			}
		case REDUCER:
			if ok := reduceProcess(reducef, reply); !ok {
				return
			}
		}
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func mapProcess(mapf func(string, string) []KeyValue, reply RegisterReply) bool {
	file, err := os.Open(reply.MapFile)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", reply.MapFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.MapFile)
	}
	kva := mapf(reply.MapFile, string(content))
	files := []*os.File{}
	for i := 0; i < reply.NReduce; i++ {
		temp, err := os.CreateTemp("", "tmp-")
		if err != nil {
			return true
		}
		defer temp.Close()
		files = append(files, temp)
	}
	for _, kv := range kva {
		fmt.Fprintf(files[ihash(kv.Key)%reply.NReduce], "%v %v\n", kv.Key, kv.Value)
	}
	args := CommitMapArgs{
		TaskID: reply.TaskID,
		Files:  nil,
	}
	//添加args参数
	for i := 0; i < reply.NReduce; i++ {
		args.Files = append(args.Files, files[i].Name())
	}
	return commitMap(&args, &CommitMapReply{})
}

func reduceProcess(reducef func(string, []string) string, reply RegisterReply) bool {
	intermediate := []KeyValue{}
	for i := 0; i < len(reply.ReduceFiles); i++ {
		file, err := os.Open(reply.ReduceFiles[i])
		defer file.Close()
		if err != nil {
			return true
		}
		for {
			kv := KeyValue{}
			_, e := fmt.Fscanf(file, "%s %s\n", &kv.Key, &kv.Value)
			if e != nil {
				if e == io.EOF {
					break
				}
				return true
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reply.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		return true
	}
	defer ofile.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	args := CommitReduceArgs{
		TaskID: reply.TaskID,
	}
	return commitReduce(&args, &CommitReduceReply{})
}

func commitMap(args *CommitMapArgs, reply *CommitMapReply) bool {
	return call("Coordinator.CommitMap", args, reply)
}

func commitReduce(args *CommitReduceArgs, reply *CommitReduceReply) bool {
	return call("Coordinator.CommitReduce", args, reply)
}
