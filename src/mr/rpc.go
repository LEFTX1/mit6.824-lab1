package mr

//
// RPC定义。
//
// 记得将所有名称首字母大写。
//

import "os"
import "strconv"

//
// 示例，展示如何声明RPC的参数
// 和回复。
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// 在此处添加你的RPC定义。

// 为master在/var/tmp中创建一个独特的UNIX域套接字名称。
// 因为Athena AFS不支持UNIX域套接字，所以不能使用当前目录。
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
