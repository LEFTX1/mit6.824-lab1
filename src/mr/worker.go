// worker.go

package mr

import (
	"encoding/json" // 导入JSON编码/解码包
	"errors"        // 导入错误处理包
	"fmt"           // 导入格式化包
	"hash/fnv"      // 导入哈希函数包
	"io/ioutil"     // 导入I/O工具包
	"log"           // 导入日志包
	"net/rpc"       // 导入RPC包
	"os"            // 导入操作系统包
	"sort"          // 导入排序包
)

// 使用 ihash(key) % NReduce 来选择每个Map阶段输出的KeyValue属于哪个Reduce任务
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker 主线程,循环请求任务以及报告任务
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// 请求任务
		reply := ReqTaskReply{}
		reply = reqTask()
		if reply.TaskDone { // 如果所有任务已完成
			break
		}
		// 执行任务
		err := doTask(mapf, reducef, reply.Task)
		if err != nil { // 如果任务执行出错，报告失败
			reportTask(reply.Task.TaskIndex, false)
		}
		// 报告任务结果
		reportTask(reply.Task.TaskIndex, true) // 如果任务成功完成，报告成功
	}
	return
}

// 请求任务
func reqTask() ReqTaskReply {
	// 声明参数并赋值
	args := ReqTaskArgs{}
	args.WorkerStatus = true // 标记Worker在线

	reply := ReqTaskReply{}

	// RPC调用，向Master请求任务
	if ok := call("Master.HandleTaskReq", &args, &reply); !ok {
		log.Fatal("请求任务失败...")
	}

	return reply // 返回任务请求的回复
}

// 报告任务结果
func reportTask(taskIndex int, isDone bool) ReportTaskReply {
	// 声明参数并赋值
	args := ReportTaskArgs{}
	args.IsDone = isDone       // 是否任务完成
	args.TaskIndex = taskIndex // 任务索引
	args.WorkerStatus = true   // 标记Worker在线

	reply := ReportTaskReply{}

	// RPC调用，向Master报告任务完成情况
	if ok := call("Master.HandleTaskReport", &args, &reply); !ok {
		log.Fatal("报告任务失败...")
	}
	return reply // 返回报告任务的回复
}

// 执行任务，根据任务阶段调用不同的处理函数
func doTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string, task Task) error {
	if task.TaskPhase == MapPhase { // 如果是Map任务
		err := DoMapTask(mapf, task.FileName, task.TaskIndex, task.ReduceNum)
		return err
	} else if task.TaskPhase == ReducePhase { // 如果是Reduce任务
		err := DoReduceTask(reducef, task.MapNum, task.TaskIndex)
		return err
	} else { // 如果任务阶段异常
		log.Fatal("请求任务的任务阶段返回值异常...")
		return errors.New("请求任务的任务阶段返回值异常")
	}
	return nil
}

// 执行Map任务
func DoMapTask(mapf func(string, string) []KeyValue, fileName string, mapTaskIndex int, reduceNum int) error {

	fmt.Println("开始处理Map任务...")
	// 打开输入文件
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("无法打开文件 %v", fileName)
		return err
	}
	// 读取文件内容
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("无法读取文件 %v", fileName)
		return err
	}
	file.Close()
	// 调用用户提供的Map函数，生成键值对
	kva := mapf(fileName, string(content))
	for i := 0; i < reduceNum; i++ {
		// 构建中间输出文件名，格式为mr-X-Y
		intermediateFileName := intermediateName(mapTaskIndex, i)
		fmt.Printf("创建中间文件 %s\n", intermediateFileName)
		// 创建中间输出文件，并使用JSON编码器写入键值对
		file, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("无法创建中间文件 %s: %v", intermediateFileName, err)
			return err
		}
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			if ihash(kv.Key)%reduceNum == i { // 根据哈希值决定键值对属于哪个Reduce任务
				enc.Encode(&kv) // 将键值对写入文件
			}
		}
		file.Close()
	}
	return nil // 返回nil表示成功
}

// 执行Reduce任务
func DoReduceTask(reducef func(string, []string) string, mapNum int, reduceTaskIndex int) error {
	fmt.Println("开始处理Reduce任务...")
	// 创建一个Map用于存储所有键及其对应的值列表
	res := make(map[string][]string)
	for i := 0; i < mapNum; i++ {
		// 打开对应的中间文件
		intermediateFileName := intermediateName(i, reduceTaskIndex)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("无法打开中间文件 %v", intermediateFileName)
			return err
		}
		// 创建JSON解码器
		dec := json.NewDecoder(file)
		// 读取文件内容并反序列化为KeyValue
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil { // 读取完毕或出错退出循环
				break
			}
			_, ok := res[kv.Key]
			if !ok {
				res[kv.Key] = make([]string, 0) // 如果键不存在，初始化值列表
			}
			res[kv.Key] = append(res[kv.Key], kv.Value) // 将值添加到对应键的值列表
		}
		file.Close()
	}
	// 提取所有键，用于排序
	var keys []string
	for k := range res {
		keys = append(keys, k)
	}
	// 对键进行排序
	sort.Strings(keys)
	// 构建输出文件名，格式为mr-out-Y
	outputFileName := outputName(reduceTaskIndex)
	fmt.Printf("生成输出文件 %s\n", outputFileName)
	// 创建输出文件
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("无法创建输出文件 %s: %v", outputFileName, err)
		return err
	}
	defer outputFile.Close()

	for _, k := range keys {
		output := reducef(k, res[k])                  // 调用用户提供的Reduce函数
		fmt.Fprintf(outputFile, "%v %v\n", k, output) // 写入到输出文件
	}

	return nil // 返回nil表示成功
}

// call 发送一个RPC请求给Master，并等待响应。
// 通常返回true。如果出现问题，返回false。

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := masterSock() // 使用统一的Socket名称
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("RPC Dial error:", err)
		return false
	}
	defer c.Close()

	// 调用RPC方法
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println("RPC Call error:", err)
	return false
}

// intermediateName 生成中间文件名，格式为mr-X-Y
// X是Map任务编号，Y是Reduce任务编号
func intermediateName(mapTaskIndex int, reduceTaskIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskIndex, reduceTaskIndex)
}

// outputName 生成输出文件名，格式为mr-out-Y
// Y是Reduce任务编号
func outputName(reduceTaskIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceTaskIndex)
}
