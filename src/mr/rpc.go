// rpc.go

package mr

// 定义键值对结构体，用于Map和Reduce阶段
type KeyValue struct {
	Key   string
	Value string
}

// 请求任务参数结构体
type ReqTaskArgs struct {
	WorkerStatus bool // 当前Worker在线，可以接受任务
}

// 请求任务回复结构体
type ReqTaskReply struct {
	Task     Task // 返回一个任务
	TaskDone bool // 是否完成所有任务
}

// 报告任务参数结构体
type ReportTaskArgs struct {
	WorkerStatus bool // 当前Worker在线
	TaskIndex    int  // 任务索引
	IsDone       bool // 任务是否完成
}

// 报告任务回复结构体
type ReportTaskReply struct {
	MasterAck bool // Master确认是否成功处理报告
}
