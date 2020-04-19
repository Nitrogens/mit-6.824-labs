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
	isMapTaskAssigned       []bool
	isReduceTaskAssigned    []bool
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
	log.Println("[GetTask] start")
	var taskInfo Task
	resp.TotalReduceTaskCount = m.totalReduceTaskCount
	m.mu.Lock()
	defer m.mu.Unlock()
	for idx, mapTask := range m.mapTaskList {
		if m.isMapTaskFinished[idx] {
			continue
		}
		log.Println("[MapTaskReset]", idx, mapTask)
		timeDelta := time.Since(mapTask.AssignedTime)
		if timeDelta >= kTimeThreshold && m.isMapTaskAssigned[idx] {
			m.assignedMapTaskCount--
			m.isMapTaskAssigned[idx] = false
			m.mapTaskList[idx].AssignedTime = time.Time{}
			m.mapTaskChan <- m.mapTaskList[idx]
		}
	}
	for idx, reduceTask := range m.reduceTaskList {
		if m.isReduceTaskFinished[idx] {
			continue
		}
		log.Println("[ReduceTaskReset]", idx, reduceTask)
		timeDelta := time.Since(reduceTask.AssignedTime)
		if timeDelta >= kTimeThreshold && m.isReduceTaskAssigned[idx] {
			m.assignedReduceTaskCount--
			m.isReduceTaskAssigned[idx] = false
			m.reduceTaskList[idx].AssignedTime = time.Time{}
			m.reduceTaskChan <- m.reduceTaskList[idx]
		}
	}
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
	if taskInfo.Type == kMapTask {
		m.mapTaskList[taskInfo.ID].AssignedTime = taskInfo.AssignedTime
		m.isMapTaskAssigned[taskInfo.ID] = true
	} else if taskInfo.Type == kReduceTask {
		m.reduceTaskList[taskInfo.ID].AssignedTime = taskInfo.AssignedTime
		m.isReduceTaskFinished[taskInfo.ID] = true
	}
	resp.StatusCode = kStatusCodeOK
	resp.TaskInfo = taskInfo
	log.Printf("[GetTask] end: %+v", resp)
	return nil
}

func (m *Master) SetTaskFinished(req *SetTaskFinishedReq, resp *SetTaskFinishedResp) error {
	log.Printf("[SetTaskFinished] start: %+v", req)
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
					FileNames: make([]string, 0, m.totalMapTaskCount),
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
	log.Printf("[SetTaskFinished] end: %+v | %+v", req, resp)
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
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.finishedReduceTaskCount == m.totalReduceTaskCount {
		ret = true
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
	m.mapTaskChan = make(chan Task, m.totalMapTaskCount)
	m.reduceTaskChan = make(chan Task, m.totalReduceTaskCount)
	m.mapTaskList = make([]Task, 0, m.totalMapTaskCount)
	m.reduceTaskList = make([]Task, 0, m.totalReduceTaskCount)
	m.isMapTaskAssigned = make([]bool, m.totalMapTaskCount)
	m.isReduceTaskAssigned = make([]bool, m.totalReduceTaskCount)
	m.isMapTaskFinished = make([]bool, m.totalMapTaskCount)
	m.isReduceTaskFinished = make([]bool, m.totalReduceTaskCount)

	for idx, fileName := range files {
		log.Println(idx, fileName)
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
