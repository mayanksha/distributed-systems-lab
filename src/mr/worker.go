package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	var allReducesDoneChan chan bool = make(chan bool, 1)

	go getAndProcessMapJob(allMapsDoneChan, mapf)

	for allMapsDone := range allMapsDoneChan {
		if allMapsDone {
			break
		}
		// If all the maps are not done, we'll check with the coordinator again
		// But Let's wait for some time before pinging the coordinator
		time.Sleep(time.Millisecond * 10)

		go getAndProcessMapJob(allMapsDoneChan, mapf)
	}

	fmt.Println("[Worker] All the map jobs have been done. Starting Reduce step now.")
	fmt.Printf("****************************************************\n\n\n")

	go getAndProcessReduceJob(allReducesDoneChan, reducef)
	for allReducesDone := range allReducesDoneChan {
		if allReducesDone {
			break
		}

		// If all the reduces are not done, we'll check with the coordinator again
		// But Let's wait for some time pinging the coordinator
		time.Sleep(time.Millisecond * 10)

		go getAndProcessReduceJob(allReducesDoneChan, reducef)
	}

}

func getReduceJobFromCoordinator() (*CoordReduceJobRequest, *CoordReduceJobReply) {
	// declare an argument structure.
	args := CoordReduceJobRequest{}

	// declare a reply structure.
	reply := CoordReduceJobReply{}

	// send the RPC request, wait for the reply.
	status := call("Coordinator.GetReduceJob", &args, &reply)
	if !status {
		log.Fatalf("[Worker] Error while getting a valid CoordReduceJobReply")
	}

	fmt.Printf("received reply from coordinator. reply: %v\n", reply)

	return &args, &reply
}

func getAndProcessReduceJob(ch chan bool, reducef func(string, []string) string) {
	_, reply := getReduceJobFromCoordinator()

	defer func(cha chan bool, r *CoordReduceJobReply) {
		cha <- r.Status == ALL_DONE
	}(ch, reply)

	fmt.Printf("[Worker] Reduce -- Reply before check: %v\n", reply)
	if reply.Status == ALL_DONE || (reply.Status == WAIT_FOR_OTHERS && len(reply.Files) == 0) {
		return
	}

	// For now, in each call to the Coordinator, we'll only send a single file in the reply
	if len(reply.Files) > 1 {
		log.Fatalf("[Worker] Error. Only one file needs to be there in CoordReduceJobReply.")
	}

	filePath := reply.Files[0]
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("[Worker] Error. Cannot open %v. err: %v", filePath, err)
	}

	var kva []KeyValue
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		separated := strings.Split(line, ",")
		kva = append(kva, KeyValue{Key: separated[0], Value: separated[1]})
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	file.Close()

	// Open the output files
	dir, _ := os.Getwd()
	var outFiles []*os.File
	for i := 0; i < reply.NReduce; i++ {
		outFilePath := path.Join(dir, fmt.Sprintf("mr-out-%d", i))
		ofile, err := os.OpenFile(outFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("[Worker] Error. Cannot create file at path %v", outFilePath)
		}

		outFiles = append(outFiles, ofile)
		defer ofile.Close()
	}

	// Sort the kva by Key
	sort.Sort(ByKey(kva))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		fileIndexToWrite := getHash(kva[i].Key, reply.NReduce)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFiles[fileIndexToWrite], "%v %v\n", kva[i].Key, output)

		i = j
	}

	markReduceJobDone(reply)
	fmt.Printf("[REDUCE] Successfully marked the file as done. job: %v\n", reply)

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

func getAndProcessMapJob(ch chan bool, mapf func(string, string) []KeyValue) {
	_, reply := getMapJobFromCoordinator()

	defer func(cha chan bool, r *CoordMapJobReply) {
		cha <- r.Status == ALL_DONE
	}(ch, reply)

	fmt.Printf("[Worker] Map -- Reply before check: %v\n", reply)
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
	_ = os.MkdirAll(dirPath, 0777)

	var outFiles []*os.File
	for i := 0; i < reply.NMap; i++ {
		outFilePath := path.Join(dirPath, fmt.Sprintf("inter-out-%d", i))
		ofile, err := os.OpenFile(outFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("[Worker] Error. Cannot create file at path %v", outFilePath)
		}

		outFiles = append(outFiles, ofile)
		defer ofile.Close()
	}

	for _, val := range kva {
		index := getHash(val.Key, reply.NMap)
		fmt.Fprintf(outFiles[index], "%v,%v,%v\n", val.Key, val.Value, filename)
	}

	markMapJobDone(outFiles, reply)

	fmt.Printf("[MAP] Successfully marked the file as done. job: %v\n", reply)

}

func markReduceJobDone(job *CoordReduceJobReply) {
	// declare an argument structure.
	args := WorkerReduceJobRequest{CoordReduceJob: *job}

	// declare a reply structure.
	reply := WorkerReduceJobReply{}

	// send the RPC request, wait for the reply.
	status := call("Coordinator.MarkReduceJobDone", &args, &reply)
	if !status {
		log.Fatalf("[Worker] Error while updating the status of current job. Job: %v", args.CoordReduceJob)
	}
}

func markMapJobDone(tempFiles []*os.File, job *CoordMapJobReply) {
	var absPaths []string
	for _, val := range tempFiles {
		absPaths = append(absPaths, val.Name())
	}

	// declare an argument structure.
	args := WorkerMapJobRequest{CoordMapJob: *job, TempFilePaths: absPaths}

	// declare a reply structure.
	reply := WorkerMapJobReply{}

	// send the RPC request, wait for the reply.
	status := call("Coordinator.MarkMapJobDone", &args, &reply)
	if !status {
		log.Fatalf("[Worker] Error while updating the status of current job. Job: %v", args.CoordMapJob)
	}
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
