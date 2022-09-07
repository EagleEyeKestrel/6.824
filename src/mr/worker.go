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

//
// Map functions return a slice of KeyValue.
//
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
		if task.TaskState == Working {
			if task.TaskType == MapTask {
				DoMap(mapf, task)
				mapDone(task)
			} else {
				DoReduce(reducef, task)
				reduceDone(task)
			}
		} else if task.TaskState == Waiting { //如果是map的waiting，但是map已经结束，先传waiting过来，但是master已经进入reduce
			//fmt.Println("All tasks have been assigned, waiting...")
			time.Sleep(time.Second)
		} else if task.TaskState == Exit {
			//fmt.Println("Task ", task.TaskID, " has been done.")
			break
		}
	}
}

func DoMap(mapf func(string, string) []KeyValue, task Task) {
	intermediate := []KeyValue{}
	fileName, reduceNum := task.FileNameList[0], task.ReduceNum
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
		path := "mr-tmp-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i)
		outputFile, _ := os.Create(path)
		enc := json.NewEncoder(outputFile)
		for _, kv := range kvs[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Save intermediate file %s failure.\n", path)
			}
		}
		outputFile.Close()
		realPath := "mr-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i)
		os.Rename(path, realPath)
	}
}

func DoReduce(reducef func(string, []string) string, task Task) {
	intermediate := []KeyValue{}
	for _, fileName := range task.FileNameList {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-tmp-out-" + strconv.Itoa(task.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}
	i := 0
	for i < len(intermediate) {
		j := i
		for j+1 < len(intermediate) && intermediate[j+1].Key == intermediate[j].Key {
			j++
		}
		values := []string{}
		for k := i; k <= j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j + 1
	}
	ofile.Close()
	realName := "mr-out-" + strconv.Itoa(task.TaskID)
	os.Rename(oname, realName)
}

func mapDone(task Task) {
	reply := struct{}{}
	ok := call("Coordinator.MarkMapDone", &task, &reply)
	if ok {
		//fmt.Printf("Task %d Done\n", task.TaskID)
	} else {
		fmt.Printf("Task %d call markMapDone failed\n", task.TaskID)
	}
}

func reduceDone(task Task) {
	reply := struct{}{}
	ok := call("Coordinator.MarkReduceDone", &task, &reply)
	if ok {
		//fmt.Printf("Task %d Done\n", task.TaskID)
	} else {
		fmt.Printf("Task %d call markReduceDone failed\n", task.TaskID)
	}
}

func GetTask() Task {
	args := struct{}{}
	reply := Task{}
	ok := call("Coordinator.GiveTask", &args, &reply)
	if ok {
		//fmt.Printf("Get task success, id: %d, state: %d\n", reply.TaskID, reply.TaskState)
	} else {
		reply.TaskState = Exit
		//fmt.Println("Get task Failed.")
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

	return false
}
