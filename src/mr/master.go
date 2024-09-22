// master.go

package mr

import (
	"errors"   // 导入错误处理包
	"fmt"      // 导入格式化包
	"log"      // 导入日志包
	"net"      // 导入网络包
	"net/http" // 导入HTTP协议包
	"net/rpc"  // 导入RPC包
	"os"       // 导入操作系统包
	"strconv"
	"sync" // 导入同步包
	"time" // 导入时间包
)

// 任务状态定义
type TaskStatus string

// 任务阶段定义
type TaskPhase string

// 定义时间持续类型
type TimeDuration time.Duration

// 任务状态常量定义
const (
	TaskStatusReady   TaskStatus = "ready"   // 就绪状态，任务已准备好被分配
	TaskStatusQueue   TaskStatus = "queue"   // 队列中，任务已经被分配但尚未开始执行
	TaskStatusRunning TaskStatus = "running" // 执行中，任务正在被Worker执行
	TaskStatusFinish  TaskStatus = "finish"  // 已完成，任务已成功完成
	TaskStatusErr     TaskStatus = "error"   // 任务错误，任务执行过程中出现错误
)

// 任务阶段常量定义
const (
	MapPhase    TaskPhase = "map"    // Map阶段
	ReducePhase TaskPhase = "reduce" // Reduce阶段
)

// 任务超时时间常量定义
const MaxTaskRunTime = 10 * time.Second // 最大任务运行时间为10秒

// 定义任务结构体
type Task struct {
	// 操作阶段：Map或Reduce
	TaskPhase TaskPhase
	// Map任务总数
	MapNum int
	// Reduce任务总数
	ReduceNum int
	// 任务序号
	TaskIndex int
	// 任务对应的文件名（仅Map任务有）
	FileName string
	// 是否完成
	IsDone bool
}

// 任务状态定义
type TaskState struct {
	// 状态
	Status TaskStatus
	// 开始执行时间
	StartTime time.Time
}

// Master结构定义
type Master struct {
	// 任务队列，用于分配任务
	TaskChan chan Task
	// 输入文件列表
	Files []string
	// Map任务数量
	MapNum int
	// Reduce任务数量
	ReduceNum int
	// 当前任务阶段（Map或Reduce）
	TaskPhase TaskPhase
	// 任务状态列表
	TaskState []TaskState
	// 互斥锁，确保并发安全
	Mutex sync.Mutex
	// 是否所有任务完成
	IsDone bool
}

// 启动Master，初始化Master结构体并返回指针
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// 初始化Master状态
	m.IsDone = false                          // 标记作业未完成
	m.Files = files                           // 设置输入文件列表
	m.MapNum = len(files)                     // 设置Map任务数量为输入文件数量
	m.ReduceNum = nReduce                     // 设置Reduce任务数量
	m.TaskPhase = MapPhase                    // 初始阶段为Map阶段
	m.TaskState = make([]TaskState, m.MapNum) // 初始化Map任务状态列表
	m.TaskChan = make(chan Task, 10)          // 创建任务通道，缓冲区大小为10
	for k := range m.TaskState {
		m.TaskState[k].Status = TaskStatusReady // 将所有Map任务状态设置为就绪
	}

	// 开启RPC服务器，监听来自Worker的请求
	m.server()

	return &m // 返回Master的指针
}
func masterSock() string {
	sockname := "/var/tmp/824-mr-"
	sockname += strconv.Itoa(os.Getuid())
	return sockname
}

// 启动一个线程，监听worker.go的RPC请求
func (m *Master) server() {
	rpc.Register(m)                      // 注册Master的RPC服务
	rpc.HandleHTTP()                     // 通过HTTP协议处理RPC请求
	sockname := masterSock()             // 使用统一的Socket名称
	os.Remove(sockname)                  // 删除已存在的Socket文件
	l, e := net.Listen("unix", sockname) // 监听统一的Socket
	if e != nil {                        // 如果监听失败
		log.Fatal("listen error:", e) // 日志记录并退出
	}
	go http.Serve(l, nil) // 开启goroutine处理HTTP请求
}

// 处理Worker的任务请求
func (m *Master) HandleTaskReq(args *ReqTaskArgs, reply *ReqTaskReply) error {
	fmt.Println("开始处理任务请求...")
	if !args.WorkerStatus { // 检查Worker是否在线
		return errors.New("当前Worker已下线")
	}
	// 从任务队列中取出一个任务
	task, ok := <-m.TaskChan
	if ok == true { // 如果取到任务
		reply.Task = task // 设置返回的任务
		// 将任务状态设置为执行中
		m.TaskState[task.TaskIndex].Status = TaskStatusRunning
		// 记录任务开始执行的时间
		m.TaskState[task.TaskIndex].StartTime = time.Now()
	} else { // 如果任务队列已关闭或无任务
		// 设置TaskDone为true，通知Worker所有任务已完成
		reply.TaskDone = true
	}
	return nil
}

// 处理Worker的任务报告
func (m *Master) HandleTaskReport(args *ReportTaskArgs, reply *ReportTaskReply) error {
	fmt.Println("开始处理任务报告...")
	if !args.WorkerStatus { // 检查Worker是否在线
		reply.MasterAck = false // 设置ACK为false
		return errors.New("当前Worker已下线")
	}
	if args.IsDone == true { // 如果任务完成
		// 将任务状态设置为已完成
		m.TaskState[args.TaskIndex].Status = TaskStatusFinish
	} else { // 如果任务执行出错
		// 将任务状态设置为错误，需要重新调度
		m.TaskState[args.TaskIndex].Status = TaskStatusErr
	}
	reply.MasterAck = true // 设置ACK为true
	return nil
}

// 循环调用 Done() 来判定任务是否完成
func (m *Master) Done() bool {
	ret := false

	finished := true                   // 初始假设所有任务已完成
	m.Mutex.Lock()                     // 上锁，保护共享资源
	defer m.Mutex.Unlock()             // 函数结束时释放锁
	for key, ts := range m.TaskState { // 遍历所有任务状态
		switch ts.Status {
		case TaskStatusReady:
			// 任务就绪，表示还未被调度
			finished = false // 标记作业未完成
			m.addTask(key)   // 将任务重新加入队列
		case TaskStatusQueue:
			// 任务在队列中，表示尚未被执行
			finished = false // 标记作业未完成
		case TaskStatusRunning:
			// 任务正在运行，可能还未完成
			finished = false // 标记作业未完成
			m.checkTask(key) // 检查任务是否超时
		case TaskStatusFinish:
			// 任务已完成，无需处理
		case TaskStatusErr:
			// 任务执行出错，需要重新调度
			finished = false // 标记作业未完成
			m.addTask(key)   // 将任务重新加入队列
		default:
			panic("任务状态异常...") // 遇到未知任务状态，程序崩溃
		}
	}
	// 如果所有任务都已完成
	if finished {
		// 根据当前阶段决定后续操作
		if m.TaskPhase == MapPhase { // 如果是Map阶段
			m.initReduceTask() // 初始化Reduce阶段
		} else { // 如果是Reduce阶段
			m.IsDone = true   // 标记作业完成
			close(m.TaskChan) // 关闭任务通道，通知所有Worker退出
		}
	} else {
		m.IsDone = false // 标记作业未完成
	}
	ret = m.IsDone // 返回作业完成状态
	return ret
}

// 初始化Reduce阶段，将任务状态和任务队列重新设置为Reduce任务
func (m *Master) initReduceTask() {
	m.TaskPhase = ReducePhase                    // 设置当前阶段为Reduce
	m.IsDone = false                             // 标记作业未完成
	m.TaskState = make([]TaskState, m.ReduceNum) // 初始化Reduce任务状态列表
	for k := range m.TaskState {
		m.TaskState[k].Status = TaskStatusReady // 将所有Reduce任务状态设置为就绪
	}
}

// 将指定任务索引的任务加入任务队列
func (m *Master) addTask(taskIndex int) {
	// 设置任务状态为队列中
	m.TaskState[taskIndex].Status = TaskStatusQueue
	// 构造任务信息
	task := Task{
		FileName:  "",
		MapNum:    len(m.Files),
		ReduceNum: m.ReduceNum,
		TaskIndex: taskIndex,
		TaskPhase: m.TaskPhase,
		IsDone:    false,
	}
	if m.TaskPhase == MapPhase { // 如果是Map阶段
		task.FileName = m.Files[taskIndex] // 设置任务对应的文件名
	}
	// 将任务加入任务队列
	m.TaskChan <- task
}

// 检查指定任务是否超时，如果超时则将任务重新加入队列
func (m *Master) checkTask(taskIndex int) {
	timeDuration := time.Now().Sub(m.TaskState[taskIndex].StartTime) // 计算任务运行时间
	if timeDuration > MaxTaskRunTime {                               // 如果超时
		// 任务超时，重新将任务加入队列
		m.addTask(taskIndex)
	}
}
