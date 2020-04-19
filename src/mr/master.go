package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int
type StatusType int
type StageType int

const (
	kMapTask    TaskType = 0
	kReduceTask TaskType = 1
)

const (
	kStatusCodeOK     StatusType = 0
	kStatusCodeNoTask StatusType = 1
	kStatusCodeFinish StatusType = 2
)

const (
	kTimeThreshold = time.Second * 10
)

type Master struct {
	// Your definitions here.
	mapTaskList             []Task
	reduceTaskList          []Task
	isMapTaskFinished       []bool
	isReduceTaskFinished    []bool
	mapTaskChan             chan Task
	reduceTaskChan          chan Task
	totalMapTaskCount       int
	totalReduceTaskCount    int
	assignedMapTaskCount    int
	assignedReduceTaskCount int
	finishedMapTaskCount    int
	finishedReduceTaskCount int
	mu                      sync.RWMutex
}

type Task struct {
	ID           int
	Type         TaskType
	FileNames    []string
	AssignedTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(req *GetTaskReq, resp *GetTaskResp) error {
	var taskInfo Task
	resp = &GetTaskResp{}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.assignedMapTaskCount < m.totalMapTaskCount {
		m.assignedMapTaskCount++
		taskInfo = <-m.mapTaskChan
	} else {
		if m.finishedMapTaskCount < m.totalMapTaskCount {
			resp.StatusCode = kStatusCodeNoTask
			resp.TaskInfo = Task{}
			return nil
		} else {
			if m.assignedReduceTaskCount < m.totalReduceTaskCount {
				m.assignedReduceTaskCount++
				taskInfo = <-m.reduceTaskChan
			} else {
				if m.finishedReduceTaskCount == m.totalReduceTaskCount {
					resp.StatusCode = kStatusCodeFinish
				} else {
					resp.StatusCode = kStatusCodeNoTask
				}
				resp.TaskInfo = Task{}
				return nil
			}
		}
	}
	taskInfo.AssignedTime = time.Now()
	resp.StatusCode = kStatusCodeOK
	resp.TaskInfo = taskInfo
	return nil
}

func (m *Master) SetTaskFinished(req *SetTaskFinishedReq, resp *SetTaskFinishedResp) error {
	resp = &SetTaskFinishedResp{}
	resp.StatusCode = kStatusCodeOK
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.TaskInfo.Type == kMapTask {
		m.finishedMapTaskCount++
		if m.finishedMapTaskCount == m.totalMapTaskCount {
			close(m.mapTaskChan)
			for idxReduceTask := 0; idxReduceTask < m.totalReduceTaskCount; idxReduceTask++ {
				task := Task{
					ID:        idxReduceTask,
					Type:      kReduceTask,
					FileNames: make([]string, m.totalMapTaskCount),
				}
				for idxMapTask := 0; idxMapTask < m.totalMapTaskCount; idxMapTask++ {
					task.FileNames = append(task.FileNames, fmt.Sprintf("mr-%v-%v", idxMapTask, idxReduceTask))
				}
				task.AssignedTime = time.Now()
				m.reduceTaskList = append(m.reduceTaskList, task)
				m.isReduceTaskFinished[idxReduceTask] = false
				m.reduceTaskChan <- task
			}
		}
		m.isMapTaskFinished[req.TaskInfo.ID] = true
	} else if req.TaskInfo.Type == kReduceTask {
		m.finishedReduceTaskCount++
		if m.finishedReduceTaskCount == m.totalReduceTaskCount {
			close(m.reduceTaskChan)
		}
		m.isReduceTaskFinished[req.TaskInfo.ID] = true
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.finishedReduceTaskCount == m.totalReduceTaskCount {
		ret = true
	} else {
		if m.finishedMapTaskCount < m.totalMapTaskCount {
			for idx, mapTask := range m.mapTaskList {
				timeDelta := time.Since(mapTask.AssignedTime)
				if timeDelta >= kTimeThreshold {
					m.assignedMapTaskCount--
					m.mapTaskList[idx].AssignedTime = time.Now()
					m.mapTaskChan <- m.mapTaskList[idx]
				}
			}
		} else {
			for idx, reduceTask := range m.reduceTaskList {
				timeDelta := time.Since(reduceTask.AssignedTime)
				if timeDelta >= kTimeThreshold {
					m.assignedReduceTaskCount--
					m.reduceTaskList[idx].AssignedTime = time.Now()
					m.reduceTaskChan <- m.mapTaskList[idx]
				}
			}
		}
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.totalMapTaskCount = len(files)
	m.totalReduceTaskCount = nReduce
	m.mapTaskList = make([]Task, 0, m.totalMapTaskCount)
	m.reduceTaskList = make([]Task, 0, m.totalReduceTaskCount)
	m.isMapTaskFinished = make([]bool, m.totalMapTaskCount)
	m.isReduceTaskFinished = make([]bool, m.totalReduceTaskCount)

	for idx, fileName := range files {
		task := Task{
			ID:        idx,
			Type:      kMapTask,
			FileNames: []string{fileName},
		}
		m.mapTaskList = append(m.mapTaskList, task)
		m.isMapTaskFinished[idx] = false
		m.mapTaskChan <- task
	}

	m.server()
	return &m
}
