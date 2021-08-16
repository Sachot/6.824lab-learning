package mr

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// GOPATH="/Users/caizx/go"
// GOROOT="/usr/local/Cellar/go@1.13/1.13.15/libexec"
// master收到请求任务：HandleTaskReq()
// master收到worker的任务完成情况：HandleTaskRes()
type Master struct {
	// Your definitions here.
	State int  //MASTER_INIT; MAP_FINISH; REDUCE_FINISH
	//互斥锁，master为单线程，没必要在Done()上锁
	//Mutex sync.Mutex
	ReduceNum int
	MapNum int
	MapTask []Task
	ReduceTask []Task
	MapDone bool
	ReduceDone bool
	Files []string
}

const(
	MASTER_INIT int = 1
	MAP_FINISHED int = 2
	REDUCE_FINISHED  int = 3
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// master收到请求任务：HandleTaskReq()
func (m *Master) HandleTaskReq(args *GetTaskArgs, reply *AssignTaskReply) error {
	noTask := false
	//fmt.Println("接收到worker处理任务请求----")
	if !args.WorkerState {
		return errors.New("该worker无法处理任务")
	}
	if m.State == MASTER_INIT {
		for i := range m.MapTask {
			if m.MapTask[i].State == TaskReady {
				//fmt.Println("开始执行map任务："+strconv.Itoa(m.MapTask[i].Index))
				reply.Phase = Map
				reply.AssignTask = m.MapTask[i]
				reply.NoTask = false
				m.MapTask[i].State = TaskRunning
				m.MapTask[i].StartTime = time.Now()
				break
			}
			if (i == (len(m.MapTask)-1)) && (m.MapTask[i].State!=TaskReady) {
				noTask = true
			}
		}
		if noTask {  // 仍有map任务在running中
			reply.Phase = Map
			reply.NoTask = true
		}
		return nil
	}else if m.State == MAP_FINISHED {
		for i := range m.ReduceTask {
			if m.ReduceTask[i].State == TaskReady {
				reply.Phase = Reduce
				reply.AssignTask = m.ReduceTask[i]
				reply.NoTask = false
				m.ReduceTask[i].State = TaskRunning
				m.ReduceTask[i].StartTime = time.Now()
				break
			}
			if (i == (len(m.ReduceTask)-1)) && (m.ReduceTask[i].State!=TaskReady) {
				noTask = true
			}
		}
		if noTask {
			reply.Phase = Reduce
			reply.NoTask = true
		}
		return nil
	}else {
		reply.Phase = Finish
		reply.NoTask = true
		return nil
	}
}

// master收到worker的任务完成情况：HandleTaskRes()
func (m *Master) HandleTaskRes(args *ResTaskArgs, reply *ResTaskReply) error {
	//fmt.Println("接收到worker处理任务反馈----")
	if !args.WorkerState {
		return errors.New("该worker无法处理任务")
	}
	if args.TaskType==Map { // map任务的反馈
		if args.TaskIsDone {
			m.MapTask[args.TaskIndex].State = TaskFinish
		}else {
			m.MapTask[args.TaskIndex].State = TaskReady
		}
	}else { // reduce任务的反馈
		if args.TaskIsDone {
			m.ReduceTask[args.TaskIndex].State = TaskFinish
		}else {
			m.ReduceTask[args.TaskIndex].State = TaskReady
		}
	}
	reply.MasterAck = true
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
	m.MapDone = true
	m.ReduceDone = true
	//m.Mutex.Lock()
	//defer m.Mutex.Unlock()

	if m.State == MASTER_INIT {
		for i:=0;i< len(m.MapTask);i++ {
			switch m.MapTask[i].State {
			case TaskReady:
				//fmt.Println("map任务等待执行："+strconv.Itoa(m.MapTask[i].Index))
				// 等待执行
				m.MapDone = false
			case TaskFinish:
				// 任务完成
			case TaskRunning:
				// 正在执行，检查是否超时
				m.MapDone = false
				m.CheckTask(&m.MapTask[i])
			/*case TaskTimeOut:
			// 超时
			mapDone = false
			m.AddTask(m.MapTask[i].Index, Map)*/
			default:
				panic("map任务状态异常...")
			}
		}
	}

	// map任务完成，初始化reduce
	if m.MapDone && m.State == MASTER_INIT {
		m.State = MAP_FINISHED
		m.ReduceTask = make([]Task, m.ReduceNum)
		for i := range m.ReduceTask {
			m.ReduceTask[i].Index = i
			m.ReduceTask[i].MapNum = m.MapNum
			m.ReduceTask[i].TaskType = Reduce
			m.ReduceTask[i].State = TaskReady
		}
	}

	if m.State == MAP_FINISHED {
		for j := range m.ReduceTask {
			switch m.ReduceTask[j].State {
			case TaskReady:
				// 等待执行
				m.ReduceDone = false
			case TaskFinish:
				// 任务完成
			case TaskRunning:
				// 正在执行，检查是否超时
				m.ReduceDone = false
				m.CheckTask(&m.ReduceTask[j])
			/*case TaskTimeOut:
			// 超时
			mapDone = false
			m.AddTask(m.MapTask[i].Index, Map)*/
			default:
				panic("reduce任务状态异常...")
			}
		}
	}

	if m.MapDone && m.ReduceDone && m.State == MAP_FINISHED {
		m.State = REDUCE_FINISHED
		ret = true
	}

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.State = MASTER_INIT
	m.Files = files
	m.MapDone = false
	m.MapNum = len(files)
	m.MapTask = make([]Task, m.MapNum)
	for i:=0;i<m.MapNum;i++ {
		m.MapTask[i].ReduceNum = nReduce
		m.MapTask[i].MapNum = m.MapNum
		m.MapTask[i].TaskType = Map
		m.MapTask[i].Index = i
		m.MapTask[i].InputFileName = files[i]
		m.MapTask[i].IsDone = false
		m.MapTask[i].State = TaskReady
		//未开始执行，未开始计时
	}
	m.ReduceDone = false
	m.ReduceNum = nReduce
	m.ReduceTask = nil

	// Your code here.

	// 开启监听
	m.server()
	return &m
}

func (m *Master) CheckTask (task *Task) {
		timeDuration := time.Now().Sub(task.StartTime)
		if timeDuration > (10*time.Second) {
			//任务超时处理
			task.State = TaskReady
			// fmt.Println("超时重新执行:"+strconv.Itoa(task.Index))  re-execute
			fmt.Println("re-execute,task:"+strconv.Itoa(task.Index))

			/*if task.TaskType==Map {
				m.AddTask(task.Index, Map)
			}else if task.TaskType==Reduce {
				m.AddTask(task.Index, Reduce)
			}*/
		}
}

/*func (m *Master) AddTask (taskIndex int, taskType int) {
	if taskType==Map { // map任务
		task := Task{
			TaskType: Map,
			State: TaskReady,
			InputFileName: m.Files[taskIndex],
			Index: taskIndex,
			MapNum: m.MapNum,
			IsDone: false,
		}
		m.MapTask = append(m.MapTask, task)
	} else if taskType==Reduce { // reduce任务
		task := Task{
			TaskType: Reduce,
			State: TaskReady,
			Index: taskIndex,
			IsDone: false,
		}
		m.ReduceTask = append(m.ReduceTask, task)
	}
}*/

