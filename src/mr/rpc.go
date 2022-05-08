package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// My code

const (
	NOT_STARTED     = 0
	PROCESSING      = 1
	TIMED_OUT       = 2
	DONE            = 3
	WAIT_FOR_OTHERS = 4
	ALL_DONE        = 5
)

type WorkerMapJobRequest struct {
	CoordMapJob CoordMapJobReply
}

type WorkerMapJobReply struct{}

type CoordMapJobRequest struct{}

type CoordMapJobReply struct {
	Id      int // outputs inter-out-${id} file
	Status  int // Current status of the job (as seen by coordinator) -- means whether all the map jobs have completed successfully or not
	NReduce int // number of workers performing the reduce step
	Files   []string
}

type ReduceJobRequest struct{}

type ReduceJobReply struct {
	Id int // corresponds to inter-out-${id} file
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
