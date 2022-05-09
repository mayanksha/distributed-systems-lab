package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getHash(key string, nReduce int) int {
	return ihash(key) % nReduce
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	var allMapsDoneChan chan bool = make(chan bool, 1)

	go getAndProcessMapJob(allMapsDoneChan, mapf, reducef)

	for allMapsDone := range allMapsDoneChan {
		if allMapsDone {
			break
		}
		// If all the maps are not done, we'll check with the coordinator again
		// But Let's wait a second before pinging the coordinator
		time.Sleep(time.Millisecond * 100)

		go getAndProcessMapJob(allMapsDoneChan, mapf, reducef)
	}

	fmt.Println("[Worker] All the map jobs have been done. Starting Reduce step now.")

}

func getAndProcessMapJob(ch chan bool, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	_, reply := getMapJobFromCoordinator()

	defer func(cha chan bool, r *CoordMapJobReply) {
		cha <- r.Status == ALL_DONE
	}(ch, reply)

	fmt.Printf("[Worker] Reply before check: %v\n", reply)
	if reply.Status == ALL_DONE || (reply.Status == WAIT_FOR_OTHERS && len(reply.Files) == 0) {
		return
	}

	// For now, in each call to the Coordinator, we'll only send a single file in the reply
	if len(reply.Files) > 1 {
		log.Fatalf("[Worker] Error. Only one file needs to be there in CoordMapJobReply.")
	}

	filename := reply.Files[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("[Worker] Error. Cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[Worker] Error. cannot read %v", filename)
	}

	file.Close()

	kva := mapf(filename, string(content))

	dir, _ := os.Getwd()
	dirPath := path.Join(dir, "intermediate-output")
	err = os.Mkdir(dirPath, 0777)

	/* if err != nil {
	 *     fmt.Printf("[Worker] Error. Couldn't create directory with path: %v", dirPath)
	 * } */

	intermediateFilePath := path.Join(dirPath, fmt.Sprintf("inter-out-%d", reply.Id))
	ofile, err := os.Create(intermediateFilePath)
	if err != nil {
		log.Fatalf("[Worker] Error. Cannot create file at path %v", intermediateFilePath)
	}

	for _, val := range kva {
		fmt.Fprintf(ofile, "%v %v\n", val.Key, filename)
	}

	ofile.Close()

	markMapJobDone(reply)

}

func markMapJobDone(job *CoordMapJobReply) {
	// declare an argument structure.
	args := WorkerMapJobRequest{CoordMapJob: *job}

	// declare a reply structure.
	reply := WorkerMapJobReply{}

	// send the RPC request, wait for the reply.
	status := call("Coordinator.MarkMapJobDone", &args, &reply)
	if !status {
		log.Fatalf("[Worker] Error while updating the status of current job. Job: %v", args.CoordMapJob)
	}
}

func getMapJobFromCoordinator() (*CoordMapJobRequest, *CoordMapJobReply) {
	// declare an argument structure.
	args := CoordMapJobRequest{}

	// declare a reply structure.
	reply := CoordMapJobReply{}

	// send the RPC request, wait for the reply.
	status := call("Coordinator.GetMapJob", &args, &reply)
	if !status {
		log.Fatalf("[Worker] Error while getting a valid CoordMapJobReply")
	}

	fmt.Printf("received reply from coordinator. reply: %v\n", reply)

	return &args, &reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
