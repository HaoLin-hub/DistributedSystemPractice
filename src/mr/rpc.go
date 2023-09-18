package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// -------------------- Add your RPC definitions here-------------------

// PlainArgs 类型充当一下参数，实际上没什么用
type PlainArgs struct {
}

// 由coordinator返回给work的分配给它的任务信息
type TaskType int
type Task struct {
	TaskType    TaskType
	TaskId      int
	NReduce     int // 传入的reducer的数量，用于hash
	SplitFile   string
	ReduceFiles []string
}

type Phase int // Phase 对于分配任务阶段的父类型
type State int // State 任务的状态的父类型

// 枚举任务的类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask // Waitting任务代表此时为任务都分发完了，但是任务还没完成，阶段未改变
	ExitTask     // exit
)

// 枚举阶段的类型
const (
	MapPhase    Phase = iota // 此阶段在分发MapTask
	ReducePhase              // 此阶段在分发ReduceTask
	AllDone                  // 此阶段已完成
)

// 任务状态类型
const (
	Working State = iota // 此阶段在工作(in-progess)
	Waiting              // 此阶段在等待执行(idle)
	Done                 // 此阶段已经做完(completed)
)

// Cook up a unique-ish UNIX-domain socket name in /var/tmp, for the coordinator.
// Can't use the current directory since Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
