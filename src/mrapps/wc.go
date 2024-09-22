package main

//
// 一个用于 MapReduce 的词频统计插件程序。
//
// 使用以下命令构建插件：
// go build -buildmode=plugin wc.go
//

import "../mr"   // 引入 MapReduce 包
import "unicode" // 引入 unicode 包，用于处理字符
import "strings" // 引入 strings 包，用于字符串操作
import "strconv" // 引入 strconv 包，用于字符串和其他类型的转换

//
// Map 函数对每个输入文件调用一次。第一个参数是输入文件的名称，
// 第二个参数是文件的完整内容。可以忽略文件名，只处理内容。
// 返回值是一个键/值对的切片（数组）。
//
func Map(filename string, contents string) []mr.KeyValue {
	// 定义一个用于检测单词分隔符的函数。
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// 使用分隔符函数将内容分割成单词数组。
	words := strings.FieldsFunc(contents, ff)

	// 创建一个空的键值对数组。
	kva := []mr.KeyValue{}
	for _, w := range words {
		// 对于每个单词，创建一个键值对，值为 "1"。
		kv := mr.KeyValue{w, "1"}
		// 将键值对追加到数组中。
		kva = append(kva, kv)
	}
	// 返回键值对数组。
	return kva
}

//
// Reduce 函数对每个由 Map 任务生成的键调用一次，传入的是该键对应的所有值的列表。
//
func Reduce(key string, values []string) string {
	// 返回这个单词出现的次数，也就是 values 数组的长度。
	return strconv.Itoa(len(values))
}
