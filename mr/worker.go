package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key)) // 非加密哈希算法，将字符串key映射为一个数字
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 启动worker
	for {
		// worker从master获取任务
		task := getTask() // 执行远程调用并返回结果

		// 拿到task之后，根据task的state，map task交给mapper， reduce task交给reducer
		// 额外加两个state，让 worker 等待 或者 直接退出
		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}
}

func reducer(task *Task, reducef func(string, []string) string) {
	//先从filepath读取intermediate的KeyValue
	intermediate := *readFromLocalFile(task.Intermediates)
	//根据kv排序
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	// 这部分代码修改自mrsequential.go
	i := 0
	for i < len(intermediate) {
		//将相同的key放在一起分组合并
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//交给reducef，拿到结果
		output := reducef(intermediate[i].Key, values)
		//写到对应的output文件
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tempFile.Name(), oname)
	task.Output = oname
	TaskCompleted(task)
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {
	//从文件名读取content
	content, err := ioutil.ReadFile(task.Input) // 读取map输入
	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}
	//将content交给mapf，缓存结果
	intermediates := mapf(task.Input, string(content)) // 将文件名和文件内容一起传给map函数

	//缓存后的结果会写到本地磁盘，并切成R份
	//切分方式是根据key做hash
	buffer := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}
	mapOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskNumber, i, &buffer[i])) // 将kvs写入文件中，并返回路径名
	}
	//R个文件的位置发送给master
	task.Intermediates = mapOutput
	TaskCompleted(task)
}

func getTask() Task {
	// worker从master获取任务
	args := ExampleArgs{}
	reply := Task{}
	call("Master.AssignTask", &args, &reply) // 调用master的Master.AssignTask方法
	return reply
}

func TaskCompleted(task *Task) {
	//通过RPC，把task信息发给master
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply) // rpc调用
}

func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()                              // 获取当前工作目录的路径。
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*") // 在dir目录下创建一个临时文件，以 "mr-tmp-" 开头，后面跟随一串随机字符，以确保唯一性。
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFile) // 创建一个encoder
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil { // 对kv进行编码，并将结果写入tempFile文件中
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName) // 将文件名重命名为outputName
	return filepath.Join(dir, outputName)  // 将目录路径和文件名拼接成一个完整的文件路径
}

func readFromLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Failed to open file "+filepath, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := masterSock()                 // unix 套接字
	c, err := rpc.DialHTTP("unix", sockname) // 连接远程RPC服务，成功则c将成为一个指向该服务的客户端对象
	if err != nil {
		// Master结束进程，退出worker
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply) // 执行远程调用
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
