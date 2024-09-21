package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

// Map函数返回一个KeyValue切片。
type KeyValue struct {
	Key   string
	Value string
}

// 使用 ihash(key) % NReduce 来选择每个Map产生的KeyValue所属的 reduce
// 任务编号。
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go 调用此函数。
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 在此处添加你的worker实现。

	// 取消注释以向master发送示例RPC。
	// CallExample()

}

// 示例函数，展示如何向master进行RPC调用。
//
// RPC的参数和回复类型在rpc.go中定义。
func CallExample() {

	// 声明一个参数结构体。
	args := ExampleArgs{}

	// 填充参数。
	args.X = 99

	// 声明一个回复结构体。
	reply := ExampleReply{}

	// 发送RPC请求，等待回复。
	call("Master.Example", &args, &reply)

	// reply.Y 应该为100。
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// 发送一个RPC请求给master，等待响应。
// 通常返回true。
// 如果出现问题，返回false。
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("拨号错误:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
