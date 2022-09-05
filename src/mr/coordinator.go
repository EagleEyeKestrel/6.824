package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mapNum         int
	reduceNum      int
	fileList       []string
	nowTaskID      int
	mapTaskList    []*Task
	reduceTaskList []*Task
	mapTaskPtr     int
	reduceTaskPtr  int
	//mapTaskChannel    chan *Task
	//reduceTaskChannel chan *Task
	phase CoordinatorPhase
}

var lock sync.Mutex

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	lock.Lock()
	defer lock.Unlock()
	//fmt.Println(c.phase)
	return c.phase == Done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.fileList = files
	c.mapNum = len(files)
	c.reduceNum = nReduce
	c.nowTaskID = 0
	c.mapTaskPtr = 0
	c.reduceTaskPtr = 0
	//c.mapTaskChannel = make(chan *Task, len(files))
	//c.ReduceTaskChannel = make(chan *Task, )
	c.CreateMapTasks(files)
	c.server()
	return &c
}

func (c *Coordinator) CreateMapTasks(files []string) {
	for _, fileName := range files {
		taskID := c.nowTaskID
		c.nowTaskID++
		task := Task{}
		task.TaskID = taskID
		task.TaskType = MapTask
		task.ReduceNum = c.reduceNum
		task.TaskState = Begin
		task.FileName = fileName

		c.mapTaskList = append(c.mapTaskList, &task)
		//c.mapTaskChannel <- &task
	}
}

func (c *Coordinator) GiveTask(args *struct{}, reply *Task) error {
	lock.Lock()
	defer lock.Unlock()
	if c.phase == Map {
		if c.mapTaskPtr < c.mapNum {
			*reply = *c.mapTaskList[c.mapTaskPtr]
			c.mapTaskPtr++
		} else {
			if c.checkAllTasks(c.mapTaskList) {
				c.mapToReduce()
			}
			fmt.Println(c.phase)
			reply.TaskState = Waiting
		}
	} else {
		reply.TaskState = Exit
	}
	return nil
}

func (c *Coordinator) checkAllTasks(taskList []*Task) bool {
	for _, task := range taskList {
		if task.TaskState != Waiting {
			return false
		}
	}
	return true
}

func (c *Coordinator) mapToReduce() {
	//c.phase = Reduce
	c.phase = Done
	//c.CreateReduceTasks()
}

func (c *Coordinator) MarkMapDone(task *Task, reply *struct{}) error {
	taskID := task.TaskID
	c.mapTaskList[taskID].TaskState = Waiting
	return nil
}
