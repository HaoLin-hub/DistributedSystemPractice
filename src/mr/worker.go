package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// ------------------ Your worker implementation here ---------------

	for {
		task := requestTask()
		switch task.TaskType {
		case MapTask:
			doMap(mapf, &task)
			break
		case ReduceTask:
			doReduce(reducef, &task)
			break
		case WaittingTask: // 当前阶段的所有任务均已分配，等待该阶段的所有任务完成
			time.Sleep(5 * time.Second)
			break
		case ExitTask:
			fmt.Println("Coordinator的任务已经全部完成，worker退出~")
			return
		default:
			fmt.Println("这条语句如果输出了就代表程序出现严重的bug！")
			log.Fatal("fatal Error!")
			return
		}
	}
}

// 处理map任务
func doMap(mapf func(string, string) []KeyValue, task *Task) {
	filename := task.SplitFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	// 初始化将要写入的intermediate-KV 文件个数
	reduceNum := task.NReduce
	hashFile := make([][]KeyValue, reduceNum)
	// 遍历当前map任务得到的所有中间键值对，根据键将不同的Kv pair存储到R个文件中
	for _, kv := range kva {
		hashFile[ihash(kv.Key)%reduceNum] = append(hashFile[ihash(kv.Key)%reduceNum], kv)
	}
	// 遵循文档建议，将中间命名为map-map任务编号-reduce任务编号
	for i := 0; i < reduceNum; i++ {
		intermediateFileName := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		outFile, _ := os.Create(intermediateFileName)
		// 将键值对编码为json格式后输出到文件中
		enc := json.NewEncoder(outFile)
		for _, kv := range hashFile[i] {
			err := enc.Encode(kv) // 编码后写入到文件outFile中
			if err != nil {
				return
			}
		}
		outFile.Close()
	}
	// 通知coordinator当前map task已完成
	InformMapTaskDone(task)
}

func doReduce(reducef func(string, []string) string, task *Task) {
	ReduceTaskId := task.TaskId
	ReduceTaskFiles := task.ReduceFiles
	// 将属于同一个Reduce任务的键值对合并形成最终的中间KV pairs
	intermediateKva := mergeFilesAndSort(ReduceTaskFiles)
	// 创建reduce任务输出的临时文件
	dir, _ := os.Getwd()
	tempOutFile, err := os.CreateTemp(dir, "mr-tmp-*")
	fmt.Println("the temp file name is:", tempOutFile.Name())
	if err != nil {
		log.Fatal("创建临时文件失败：", err)
	}
	// 对于在intermediate数组中的每个不同key，调用reduce函数计算单词个数，并输出结果到mr-out-*文件中
	for i := 0; i < len(intermediateKva); {
		// 直到找到与inermediate[i].Key不同的索引
		j := i + 1
		for j < len(intermediateKva) && intermediateKva[j].Key == intermediateKva[i].Key {
			j++
		}
		// 此时j指向的是一个新的key
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediateKva[k].Value)
		}
		// 开始运行reduce函数返回单词个数
		output := reducef(intermediateKva[i].Key, values)
		fmt.Fprintf(tempOutFile, "%v %v\n", intermediateKva[i].Key, output)
		// 继续遍历
		i = j
	}
	tempOutFile.Close()
	newName := fmt.Sprintf("mr-out-%d", ReduceTaskId)
	os.Rename(tempOutFile.Name(), newName)

	// 通知coordinator当前map task已完成
	InformMapTaskDone(task)
}

func mergeFilesAndSort(files []string) []KeyValue {
	var kva []KeyValue
	for _, filePath := range files {
		file, _ := os.Open(filePath)
		dec := json.NewDecoder(file)
		for {
			// 将解码结果存入kv变量
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil { // 解码失败则跳出(说明该文件的所有json数据已经解码完毕)
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	// 与mrsequential.go文件的排序方式一样，按kv对的key的值从小到大的字母序排序
	sort.Sort(ByKey(kva))
	return kva
}

func InformMapTaskDone(task *Task) Task {
	args := task
	reply := Task{}
	ok := call("Coordinator.MarkDone", &args, &reply)
	if !ok {
		fmt.Println("通知失败！")
	}
	return reply
}

// 请求coordinator分配任务
func requestTask() Task {
	// 通过RPC向coordinator申请任务
	args := PlainArgs{}
	reply := Task{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		log.Println("成功申请到一个任务，当前任务Id为：", reply.TaskId, "Reduce任务的个数为：", reply.NReduce)
	} else {
		log.Println("调用失败, 未能申请到任务")
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true. returns false if something goes wrong.
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
