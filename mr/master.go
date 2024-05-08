package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MasterTaskStatus int

const (
	Idle MasterTaskStatus = iota // 空闲状态
	InProgress
	Completed
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Master struct {
	TaskQueue     chan *Task          // 等待执行的task，包括 map 任务 和 reduce 任务
	TaskMeta      map[int]*MasterTask // 当前所有task的信息
	MasterPhase   State               // Master的阶段，map、reduce、Exit
	NReduce       int
	InputFiles    []string
	Intermediates [][]string // Map任务产生的R个中间文件的信息
}

// 对map任务和reduce任务做了一个封装
type MasterTask struct {
	TaskStatus    MasterTaskStatus // InProgress、Completed、Idle
	StartTime     time.Time
	TaskReference *Task
}

type Task struct {
	Input         string // 输入文件名
	TaskState     State  // 任务所处的阶段
	NReducer      int    // reduce的个数
	TaskNumber    int
	Intermediates []string // map函数执行后结果的存放文件路径
	Output        string
}

// 互斥锁
var mu sync.Mutex

// 启动一个协程来监听来自 work 的 rpc 请求
func (m *Master) server() {
	rpc.Register(m)  // 使用 Go 语言标准库中的 RPC
	rpc.HandleHTTP() // 采用http作为RPC的载体，设置了一个默认路由，并将rpc server作为一个http handler
	//l, e := net.Listen("tcp", ":1234")

	sockname := masterSock()
	os.Remove(sockname)                  // 删除unix套接字对应地址中已存在的文件
	l, e := net.Listen("unix", sockname) // 监听 unix 本地套接字
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // 启动服务，使用默认的多路复用器
}

// main/mrmaster.go calls Exit() periodically to find out
// 判断本次 mapreduce 计算 job 是否结束
func (m *Master) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := m.MasterPhase == Exit
	return ret
}

// 创建一个 Master 并启动

// files：输入的文件
// nReduce： reduce 任务的个数
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))), // 大小为 M，R 的最大值，Go 语言中没有队列的数据结构，故使用 chan 来代替
		TaskMeta:      make(map[int]*MasterTask),
		MasterPhase:   Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	// 切成16MB-64MB的文件
	// 创建map任务
	m.createMapTask()

	// 一个程序成为 master，其他成为worker
	// 这里就是启动 master 服务器就行了，
	// 拥有 master 代码的就是 master，别的发 RPC 过来的都是 worker
	m.server()

	// 启动一个goroutine 检查超时的任务
	go m.catchTimeOut()
	return &m
}

func (m *Master) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if m.MasterPhase == Exit { //判断master协程的状态
			mu.Unlock()
			return
		}
		for _, masterTask := range m.TaskMeta { // 遍历每个task任务
			if masterTask.TaskStatus == InProgress && time.Now().Sub(masterTask.StartTime) > 10*time.Second { // 如果任务超过10s
				m.TaskQueue <- masterTask.TaskReference // 将任务重新放回任务队列
				masterTask.TaskStatus = Idle            // 并设置它的状态
			}
		}
		mu.Unlock()
	}
}

func (m *Master) createMapTask() {
	// 根据传入的filename，每个文件对应一个 map 任务
	for idx, filename := range m.InputFiles {
		taskMeta := Task{
			Input:      filename,
			TaskState:  Map,
			NReducer:   m.NReduce,
			TaskNumber: idx, // 任务号
		}
		m.TaskQueue <- &taskMeta // 将任务放进任务队列中
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,      // 设置任务初始值
			TaskReference: &taskMeta, // 对应具体的任务
		}
	}
}

// 创建reduce任务
func (m *Master) createReduceTask() {
	m.TaskMeta = make(map[int]*MasterTask) // 重置，开始保存reduce任务的信息
	for idx, files := range m.Intermediates {
		taskMeta := Task{
			TaskState:     Reduce,
			NReducer:      m.NReduce,
			TaskNumber:    idx,
			Intermediates: files,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// map任务执行时，调用此方法申请map任务
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	// assignTask就看看自己queue里面还有没有task
	mu.Lock()
	defer mu.Unlock()

	//有就发出去
	if len(m.TaskQueue) > 0 {
		*reply = *<-m.TaskQueue
		m.TaskMeta[reply.TaskNumber].TaskStatus = InProgress // 设置任务的状态
		m.TaskMeta[reply.TaskNumber].StartTime = time.Now()  // 设置任务的时间
	} else if m.MasterPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		// 没有task就让worker 等待
		*reply = Task{TaskState: Wait}
	}
	return nil
}

// map函数执行完后调用此方法
func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	//更新task状态
	mu.Lock()
	defer mu.Unlock()
	if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
		return nil
	}
	m.TaskMeta[task.TaskNumber].TaskStatus = Completed
	go m.processTaskResult(task)
	return nil
}

func (m *Master) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		//收集intermediate信息
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		if m.allTaskDone() {
			m.createReduceTask() // 所有map任务执行完成后，开始执行reduce任务
			m.MasterPhase = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			m.MasterPhase = Exit // 所有reduce任务执行完成后，设置master的状态为结束
		}
	}
}

// 判断当前持有的所有task是否执行完成
func (m *Master) allTaskDone() bool {
	for _, task := range m.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
