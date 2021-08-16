package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// worker向master请求任务：getTask
// master回应worker请求：assignTask
// worker向master报告任务完成情况：resTask
const (
	Map int = 1
	Reduce int = 2
	Finish int = 3
	TaskReady string = "ready"
	//TaskQueue string = "queue"
	TaskRunning string = "running"
	TaskFinish string = "finish"
	//TaskFail string = "fail"
	//TaskTimeOut string = "timeout"
)


// Add your RPC definitions here.
type Task struct {
	//任务类型
	TaskType int
	//任务状态
	State string
	InputFileName string
	OutputFileName string
	//任务序号
	Index int
	MapNum int
	ReduceNum int
	StartTime time.Time
	IsDone bool
}

// worker向master请求任务，请求参数
type GetTaskArgs struct {
	// 当前worker能否接收任务
	WorkerState bool
}

// master回复任务
type AssignTaskReply struct {
	Phase int
	AssignTask Task
	WorkerIndex int
	NoTask bool
}

// worker反馈任务完成情况
type ResTaskArgs struct {
	TaskType int
	TaskIndex int
	WorkerState bool
	TaskIsDone bool
}

// master接收反馈的reply
type ResTaskReply struct {
	MasterAck bool
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
