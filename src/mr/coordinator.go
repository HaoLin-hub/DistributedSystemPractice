package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

var bigLock sync.Mutex // 一把大锁用于Coordinator的互斥访问

type Coordinator struct {
	// ----------------- Your definitions here ---------------------
	ReduceNum          int               // 传入的reduce worker所需个数
	CurTaskId          int               // 当前任务编号，用于自增
	CurPhase           Phase             // 当前任务处在哪个阶段
	MapTasksChannel    chan *Task        // Map任务信道
	ReduceTasksChannel chan *Task        // Reduce任务信道
	Files              []string          // 传入的文件数组
	AllTasks           map[int]*TaskInfo // 存储所有task信息
}

//type TaskMeta struct {
//	dict  // 建立“任务id-任务元信息”的映射
//}

type TaskInfo struct {
	startTime time.Time // 任务开始的时间
	state     State     // 任务所处的状态
	task      *Task     // 指向任务的指针
}

// Your code here -- RPC handlers for the worker to call.

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	// 创建了一个监听器对象l
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	bigLock.Lock()
	defer bigLock.Unlock()
	if c.CurPhase == AllDone {
		return true
	} else {
		return false
	}
}

// create a Coordinator. nReduce is the number of reduce tasks to use.

func MakeCoordinator(files []string, nReduce int) *Coordinator {

	//------------------ Your code here -------------------
	c := Coordinator{
		Files:              files,
		ReduceNum:          nReduce,
		CurTaskId:          0,
		CurPhase:           MapPhase,
		MapTasksChannel:    make(chan *Task, len(files)),
		ReduceTasksChannel: make(chan *Task, nReduce),
		AllTasks:           make(map[int]*TaskInfo, len(files)+nReduce),
	}
	// 初始化所有map任务
	c.initMapTask(files)
	// 监听是否worker发来的消息
	c.server()

	go c.crashDetectTimer()
	return &c
}

// 将针对每个文件的处理过程都初始化为一个map任务
func (c *Coordinator) initMapTask(files []string) {
	for _, file := range files {
		// 构造一个map task
		id := GetTaskId(c)
		task := Task{
			TaskType:  MapTask,
			TaskId:    id,
			NReduce:   c.ReduceNum,
			SplitFile: file,
		}
		// 并初始化该任务的元信息
		taskInfo := TaskInfo{
			state: Waiting,
			task:  &task,
		}
		// 最后再将此原信息保存在AllTasks数组中
		if c.AllTasks[id] != nil {
			fmt.Println("AllTasks数组已包含TaskId为", id, "的任务")
			return
		}
		c.AllTasks[id] = &taskInfo
		fmt.Println("成功初始化一个map任务：", &task)
		c.MapTasksChannel <- &task
	}
}

// 初始化一个Reduce任务
func (c *Coordinator) initReduceTask() {
	fmt.Println("the function initReduceTask have run!")
	for i := 0; i < c.ReduceNum; i++ {
		id := GetTaskId(c)
		// 初始化一个Reduce任务
		task := Task{
			TaskId:      id,
			TaskType:    ReduceTask,
			ReduceFiles: getReduceFilesName(i),
		}
		// 并初始化该任务的元信息
		taskInfo := TaskInfo{
			state: Waiting,
			task:  &task,
		}
		// 最后再将此信息保存在dict中
		if c.AllTasks[id] != nil {
			fmt.Println("AllTasks数组已包含TaskId为", id, "的任务")
			return
		}
		c.AllTasks[id] = &taskInfo
		fmt.Println("成功初始化一个reduce任务：", &task)
		c.ReduceTasksChannel <- &task
	}
}

// 生成唯一的任务ID
func GetTaskId(c *Coordinator) int {
	c.CurTaskId++
	return c.CurTaskId
}

// 获取Reduce任务要处理的多个文件，这些文件是由map任务生成的
func getReduceFilesName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := os.ReadDir(path)
	// 遍历当前目录的所有文件
	for _, file := range files {
		// 判断文件名是否为mr-tmp-*-reduceNum, 若是则将该文件名加入字符串切片s
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, file.Name())
		}
	}
	return s
}

// 当worker完成任务后，通知coordinator将此任务标记为已完成
func (c *Coordinator) MarkDone(task *Task, reply *Task) error {
	bigLock.Lock()
	defer bigLock.Unlock()
	fmt.Println("call the MarkDone，the taskId：", task.TaskId)

	switch task.TaskType {
	case MapTask:
		t, ok := c.AllTasks[task.TaskId]
		if ok && t.state == Working {
			t.state = Done
			fmt.Println("TaskID为：", task.TaskId, "的Map任务被标记为完成！")
		} else {
			fmt.Println("TaskID为：", task.TaskId, "的Map任务已经完成，无需重复标记！")
		}
		break

	case ReduceTask:
		t, ok := c.AllTasks[task.TaskId]
		if ok && t.state == Working {
			t.state = Done
			fmt.Println("TaskID为：", task.TaskId, "的Reduce任务被标记为完成！")
		} else {
			fmt.Println("TaskID为：", task.TaskId, "的Reduce任务已经完成，无需重复标记！")
		}
		break

	default:
		panic("该任务类型未定义！")
	}
	return nil
}

// 当worker来申请任务时，分配相应的任务给worker
func (c *Coordinator) AssignTask(args *PlainArgs, reply *Task) error {
	bigLock.Lock()
	defer bigLock.Unlock()

	// 查看当前整个mr流程处于什么阶段
	switch c.CurPhase {
	case MapPhase:
		// 查看信道队列内是否还有map任务未取出
		if len(c.MapTasksChannel) > 0 {
			*reply = *<-c.MapTasksChannel
			if t, ok := c.AllTasks[reply.TaskId]; t.state != Waiting || !ok {
				fmt.Println("taskId为", reply.TaskId, "的任务正在运行")
			} else {
				t.state = Working
				t.startTime = time.Now()
			}
		} else {
			// map任务已经全部被分配完成，将任务状态标记为“等待任务完成”
			reply.TaskType = WaittingTask
			// 并在此任务运行的过程中，检测所有任务的完成情况，并在合适的时机进入下一阶段
			if c.checkTask() {
				c.NextPhase()
			}
		}
		break

	case ReducePhase:
		// 查看信道队列内是否还有Reduce任务未取出
		if len(c.ReduceTasksChannel) > 0 {
			*reply = *<-c.ReduceTasksChannel
			if t, ok := c.AllTasks[reply.TaskId]; t.state != Waiting || !ok {
				fmt.Println("taskId为", reply.TaskId, "的任务正在运行")
			} else {
				// 任务状态转为Working并初始化任务开始执行的时间
				t.state = Working
				t.startTime = time.Now()
			}
		} else {
			// Reduce任务已经全部被分配完成，将任务状态标记为“等待任务完成”
			reply.TaskType = WaittingTask
			// 并在此任务运行的过程中，检测所有任务的完成情况，并在合适的时机进入下一阶段
			fmt.Println("ReduceTask channel is empty, waiting for worker finish remaining reduce task")
			if c.checkTask() {
				c.NextPhase()
			}
		}
		break

	case AllDone:
		reply.TaskType = ExitTask
		break

	default:
		panic("error：流程处于未定义的阶段！")
	}
	return nil
}

func (c *Coordinator) checkTask() bool {
	var NumberOf_MapTaskDone = 0
	var NumberOf_MapTaskUnDone = 0
	var NumberOf_ReduceTaskDone = 0
	var NumberOf_ReduceTaskUnDone = 0

	// 遍历coordinator的所有任务, 查看完成情况
	for _, t := range c.AllTasks {
		if t.task.TaskType == MapTask {
			if t.state == Done {
				NumberOf_MapTaskDone++
			} else {
				NumberOf_MapTaskUnDone++
			}
		} else if t.task.TaskType == ReduceTask {
			if t.state == Done {
				NumberOf_ReduceTaskDone++
			} else {
				NumberOf_ReduceTaskUnDone++
			}
		}
	}
	fmt.Println("the number of the reduce task that have done：", NumberOf_ReduceTaskDone)
	//fmt.Println("the number of the reduce task that have undone：", NumberOf_ReduceTaskUnDone)
	// 下面将判断是否达到进入下一阶段的时机（map任务全部完成 或 reduce任务全部完成）
	if (NumberOf_MapTaskDone > 0 && NumberOf_MapTaskUnDone == 0) && (NumberOf_ReduceTaskDone == 0 && NumberOf_ReduceTaskUnDone == 0) {
		fmt.Println("will return true to into reduce phase")
		return true
	} else {
		if NumberOf_ReduceTaskDone > 0 && NumberOf_ReduceTaskUnDone == 0 {
			fmt.Println("will return true to into AllDone phase")
			return true
		}
	}
	return false
}

func (c *Coordinator) NextPhase() {
	if c.CurPhase == MapPhase {
		c.initReduceTask()
		c.CurPhase = ReducePhase
	} else if c.CurPhase == ReducePhase {
		fmt.Println("curPhase is marked AllDone!")
		c.CurPhase = AllDone
	}
}

func (c *Coordinator) crashDetectTimer() {
	for {
		// 每两秒检测一次
		time.Sleep(2 * time.Second)
		bigLock.Lock()
		// 如果mapreduce任务已经完成则直接退出coordinator
		if c.CurPhase == AllDone {
			bigLock.Unlock()
			break
		}
		// 否则逐个检查任务，查看哪个任务运行了10s还未返回
		for _, t := range c.AllTasks {
			if t.state == Working && time.Since(t.startTime) > 9*time.Second {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", t.task.TaskId, time.Since(t.startTime))

				// 将当前任务放回信道缓冲队列
				switch t.task.TaskType {
				case MapTask:
					c.MapTasksChannel <- t.task
					t.state = Waiting
					break

				case ReduceTask:
					c.ReduceTasksChannel <- t.task
					t.state = Waiting
					break
				}
			}
		}
		bigLock.Unlock()
	}
}
