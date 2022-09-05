package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	for true {
		task := GetTask()
		//fmt.Println(task.TaskState)
		if task.TaskState == Begin {
			task.TaskState = Working
			DoMap(mapf, task)
			mapDone(task)
		} else if task.TaskState == Waiting { //如果是map的waiting，但是map已经结束，先传waiting过来，但是master已经进入reduce
			fmt.Println("All tasks have been done, waiting...")
			time.Sleep(time.Second)
		} else if task.TaskState == Exit {
			//fmt.Println("Task ", task.TaskID, " has been done.")
			break
		}
	}
}

func DoMap(mapf func(string, string) []KeyValue, task Task) {
	intermediate := []KeyValue{}
	fileName, reduceNum := task.FileName, task.ReduceNum
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	file.Close()
	intermediate = mapf(fileName, string(content))
	kvs := make([][]KeyValue, reduceNum)
	for _, kv := range intermediate {
		kvs[ihash(kv.Key)%reduceNum] = append(kvs[ihash(kv.Key)%reduceNum], kv)
	}
	for i := 0; i < reduceNum; i++ {
		path := "mr-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i)
		outputFile, _ := os.Create(path)
		enc := json.NewEncoder(outputFile)
		for _, kv := range kvs[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Save intermediate file %s failure.\n", path)
			}
		}
		outputFile.Close()
	}
}

func mapDone(task Task) {
	reply := struct{}{}
	ok := call("Coordinator.MarkMapDone", &task, &reply)
	if ok {
		fmt.Printf("Task %d Done\n", task.TaskID)
	} else {
		fmt.Printf("Task %d call markMapDone failed\n", task.TaskID)
	}
}

func GetTask() Task {
	args := struct{}{}
	reply := Task{}
	ok := call("Coordinator.GiveTask", &args, &reply)
	if ok {
		fmt.Printf("Get task %d success\n", reply.TaskID)
	} else {
		reply.TaskState = Exit
		fmt.Println("Get task Failed.")
	}
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
