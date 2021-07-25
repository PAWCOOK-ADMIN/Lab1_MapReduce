package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskMeta struct {
	Filename string
	Status   TaskStatus
	NReducer int
	MapTaskNumber int
	MapOutput []string
}

type ReduceTaskMeta struct {
	Filenames []string
	Status   TaskStatus
	ReduceTaskNumber int
	ReduceOutput string
}

type TaskStatus int

const (
	MapTask TaskStatus = iota
	ReduceTask
	NoTask
)

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
