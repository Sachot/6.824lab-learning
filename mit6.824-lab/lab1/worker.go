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

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		taskReply := AssignTaskReply{}
		taskReply = GetTask()
		if taskReply.Phase == Map {
			if taskReply.NoTask {
				time.Sleep(time.Second)
				continue
			} else {
				// 处理map任务
				err := MapTask(mapf, taskReply.AssignTask)
				// 执行失败
				if err != nil {
					//fmt.Println("MapTask执行失败，返回失败信息")
					fmt.Println("MapTaskFail")
					RespondTaskCondition(Map, taskReply.AssignTask.Index, false)
				}
				// 执行成功
				RespondTaskCondition(Map, taskReply.AssignTask.Index, true)
			}
		}

		if taskReply.Phase == Reduce {
			if taskReply.NoTask {
				break
			} else {
				// 处理reduce任务
				err := ReduceTask(reducef, taskReply.AssignTask)
				// 执行失败
				if err != nil {
					//fmt.Println("ReduceTask执行失败，返回失败信息")
					fmt.Println("ReduceTaskFail")
					RespondTaskCondition(Reduce, taskReply.AssignTask.Index, false)
				}
				// 执行成功
				RespondTaskCondition(Reduce, taskReply.AssignTask.Index, true)
			}
		}
		time.Sleep(time.Second)
	}
}

func MapTask(mapf func(string, string) []KeyValue, task Task) error {
	filename := task.InputFileName
	taskIndex := task.Index
	reduceNum := task.ReduceNum
	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("MapTask cannot open %v", filename)
		return err
	}
	// 读取文件内容
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("MapTask cannot read %v", filename)
		return err
	}
	// 关闭文件
	file.Close()
	// 输入map程序
	kva := mapf(filename, string(content))
	// 中间文件的文件名格式：mr-mapTaskIndex-reduceTaskIndex
	for i:=0;i<reduceNum;i++ {
		intermidiateFileName := "mr-" + strconv.Itoa(taskIndex) + "-" + strconv.Itoa(i)

		// 不用临时文件
		//file, _ :=os.Create(intermidiateFileName)

		// 生成临时文件
		file, _ := ioutil.TempFile("/Users/caizx/mit/6.824/src/main/temp", "tmp")
		tmpFileName := file.Name()
		//fmt.Println("生成临时文件："+tmpFileName)



		// json序列化
		enc := json.NewEncoder(file)

		// 把kva写入中间文件
		for _, kv := range kva {
			if (ihash(kv.Key) % reduceNum) == i {
				enc.Encode(&kv)   // 这里不知道为什么需要传入地址
			}
		}

		// 重命名临时文件
		err := os.Rename(tmpFileName, intermidiateFileName)
		if err != nil {
			fmt.Println("生成中间文件失败："+intermidiateFileName)
			fmt.Println("Fail to produce intermediate file："+intermidiateFileName)
			fmt.Println(err)
			os.Exit(1)
		}

		file.Close()
	}
	return nil
}

func ReduceTask(reducef func(string, []string) string, task Task) error {
	taskIndex := task.Index

	// 建立一个映射：string : []string
	res := make(map[string][]string)

	// 创建输出文件：mr-out-ReduceTaskIndex
	outputFileName := "mr-out-" + strconv.Itoa(taskIndex)
	outputFile, _ := os.Create(outputFileName)

	for i:=0;i<task.MapNum;i++ {
		intermidiateFileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskIndex)
		// 打开中间文件
		file, err := os.Open(intermidiateFileName)
		if err != nil {
			log.Fatalf("ReduceTask cannot open %v", intermidiateFileName)
			return err
		}
		// json反序列化
		dec := json.NewDecoder(file)

		// 读取文件中的KeyValue，不确定该方法是否正确
		for{
			var kv KeyValue
			err := dec.Decode(&kv)
			if err!=nil {
				break
			}

			_, ok := res[kv.Key]
			if !ok { // 当ok为false表示res中没有这个映射
				res[kv.Key] = make([]string, 0)
			}
			res[kv.Key] = append(res[kv.Key], kv.Value)
		}
		file.Close()
	}
	//
	keys := []string{}
	// 对映射range遍历得到的返回值是键值对 for key, value := range res {}
	for key := range res {
		keys = append(keys, key)
	}
	// 对取得的键排序
	sort.Strings(keys)

	for _, k := range keys {
		output := reducef(k, res[k])
		// 输出至mr-out-X中
		fmt.Fprintf(outputFile, "%v %v\n", k, output)
	}
	outputFile.Close()

	return nil
}

func GetTask() AssignTaskReply {    //master: HandleTaskReq
	args := GetTaskArgs{}
	args.WorkerState = true

	reply := AssignTaskReply{}
	call("Master.HandleTaskReq", &args, &reply)
	return reply
}

func RespondTaskCondition(taskType int, taskIndex int, isDone bool) ResTaskReply {
	args := ResTaskArgs{}
	args.TaskType = taskType
	args.TaskIndex = taskIndex
	args.TaskIsDone = isDone
	args.WorkerState = true

	reply := ResTaskReply{}
	call("Master.HandleTaskRes", &args, &reply)
	return reply
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
