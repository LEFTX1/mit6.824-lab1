package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// 在此处添加你的定义。
}

// 在此处添加你的代码 -- Worker调用的RPC处理程序。
//
// 示例RPC处理程序。
//
// RPC的参数和回复类型在rpc.go中定义。
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 启动一个线程，监听来自worker.go的RPC调用
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("监听错误:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go 定期调用 Done() 以检查
// 整个作业是否已完成。
func (m *Master) Done() bool {
	ret := false

	// 在此处添加你的代码。

	return ret
}

// 创建一个Master。
// main/mrmaster.go 调用此函数。
// nReduce 是要使用的 reduce 任务数量。
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// 在此处添加你的代码。

	m.server()
	return &m
}
