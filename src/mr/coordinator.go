package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"sync"
	"time"
	"unicode"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mapNum            int
	reduceNum         int
	fileList          []string
	nowTaskID         int
	mapTaskList       []*Task
	reduceTaskList    []*Task
	mapTaskChannel    chan *Task
	reduceTaskChannel chan *Task
	phase             CoordinatorPhase
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
	c.mapTaskChannel = make(chan *Task, len(files))
	c.reduceTaskChannel = make(chan *Task, nReduce)
	c.CreateMapTasks()
	c.server()
	go c.CrashRoutine()
	return &c
}

func (c *Coordinator) CreateMapTasks() {
	for _, fileName := range c.fileList {
		taskID := c.nowTaskID
		c.nowTaskID++
		task := Task{}
		task.TaskID = taskID
		task.TaskType = MapTask
		task.ReduceNum = c.reduceNum
		task.TaskState = Begin
		task.FileNameList = []string{fileName}

		c.mapTaskList = append(c.mapTaskList, &task)
		c.mapTaskChannel <- &task
	}
}

func (c *Coordinator) CreateReduceTasks() {
	allMidFiles := make([][]string, c.reduceNum)
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fd := range files {
		fileName := fd.Name()
		if fileName[0:3] == "mr-" && unicode.IsDigit(rune(fileName[3])) {
			var last int
			for i := len(fileName) - 1; i >= 0; i-- {
				if fileName[i] == '-' {
					last = i
					break
				}
			}
			reducerID, err := strconv.Atoi(fileName[last+1:])
			if err != nil {
				log.Fatalf("Filename conversion %v error.", fileName)
			}
			allMidFiles[reducerID] = append(allMidFiles[reducerID], fileName)
		}
	}
	for reducerID := 0; reducerID < c.reduceNum; reducerID++ {
		task := Task{}
		task.TaskType = ReduceTask
		task.TaskState = Begin
		task.TaskID = reducerID
		task.ReduceNum = c.reduceNum
		task.FileNameList = allMidFiles[reducerID]

		c.reduceTaskList = append(c.reduceTaskList, &task)
		c.reduceTaskChannel <- &task
	}
}

func (c *Coordinator) GiveTask(args *struct{}, reply *Task) error {
	lock.Lock()
	defer lock.Unlock()
	if c.phase == Map {
		if len(c.mapTaskChannel) > 0 {
			task := <-c.mapTaskChannel
			task.AssignTime = time.Now()
			task.TaskState = Working
			*reply = *task
		} else {
			if c.checkAllTasks(c.mapTaskList) {
				c.mapToReduce()
			}
			reply.TaskID = -1
			reply.TaskState = Waiting
		}
	} else if c.phase == Reduce {
		fmt.Printf("Master in reduce, channel %d left\n", len(c.reduceTaskChannel))
		if len(c.reduceTaskChannel) > 0 {
			task := <-c.reduceTaskChannel
			task.AssignTime = time.Now()
			task.TaskState = Working
			*reply = *task
		} else {
			if c.checkAllTasks(c.reduceTaskList) {
				c.reduceToDone()
			}
			reply.TaskID = -1
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
	c.phase = Reduce
	c.CreateReduceTasks()
}

func (c *Coordinator) reduceToDone() {
	c.phase = Done
}

func (c *Coordinator) MarkMapDone(task *Task, reply *struct{}) error {
	lock.Lock()
	defer lock.Unlock()
	taskID := task.TaskID
	c.mapTaskList[taskID].TaskState = Waiting
	return nil
}

func (c *Coordinator) MarkReduceDone(task *Task, reply *struct{}) error {
	lock.Lock()
	defer lock.Unlock()
	taskID := task.TaskID
	c.reduceTaskList[taskID].TaskState = Waiting
	return nil
}

func (c *Coordinator) CrashRoutine() {
	for true {
		lock.Lock()
		if c.phase == Done {
			break
		}
		var taskList []*Task
		if c.phase == Map {
			taskList = c.mapTaskList
		} else {
			taskList = c.reduceTaskList
		}
		for _, task := range taskList {
			if task.TaskState == Working && time.Now().Sub(task.AssignTime) > 10*time.Second {
				fmt.Printf("Task %d has been crashed, reassign it.\n", task.TaskID)
				task.AssignTime = time.Now()
				task.TaskState = Begin
				if task.TaskType == MapTask {
					c.mapTaskChannel <- task
				} else {
					c.reduceTaskChannel <- task
				}
			}
		}
		lock.Unlock()
		time.Sleep(1 * time.Second)
	}
}
