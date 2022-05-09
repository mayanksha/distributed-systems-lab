package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sort"
	"sync"
)

type Coordinator struct {
	// Your definitions here.

	CurrMapJobID    int            // the current JOB ID for the Map JOB
	CurrReduceJobID int            // the current JOB ID for the Reduce JOB
	Lock            *sync.Mutex    // Lock for CurrMapJobID
	AllFiles        []string       // list of files
	NReduce         int            // no. of workers performing the reduce step
	NMap            int            // no. of workers performing the map step
	FilesToProcess  map[string]int // Store filename -> int i.e. status of a particular file
	IsMapReduceDone bool           // Whether both the map and reduce operations are done or not
	TempFiles       map[string]int // Temporary files
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) MarkReduceJobDone(req *WorkerReduceJobRequest, reply *WorkerReduceJobReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	filePath := req.CoordReduceJob.Files[0]
	c.TempFiles[filePath] = DONE

	fmt.Printf("[Coord] Reduce Job done. req: %v\n", req)

	return nil
}

func (c *Coordinator) MarkMapJobDone(req *WorkerMapJobRequest, reply *WorkerMapJobReply) error {
	fileName := req.CoordMapJob.Files[0]

	c.Lock.Lock()
	defer c.Lock.Unlock()
	c.FilesToProcess[fileName] = DONE

	// If the TempFiles haven't been updated by some worker yet, we updated it
	if len(c.TempFiles) == 0 {
		for _, val := range req.TempFilePaths {
			c.TempFiles[val] = NOT_STARTED
		}
		fmt.Printf("[Coord] Job done. fileName: %v, TempFilePaths: %v\n", fileName, req.TempFilePaths)
	}

	fmt.Printf("[Coord] Job done. fileName: %v\n", fileName)

	return nil
}

func getSortedKeysFromMap(m *map[string]int) []string {
	keys := make([]string, 0, len(*m))
	for k := range *m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (c *Coordinator) GetReduceJob(req *CoordReduceJobRequest, reply *CoordReduceJobReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	filePath := ""
	areAllReduceJobsDone := true

	// We need to get the sorted keys from the map because the order of iteration on a map is not guaranteed,
	// and especially so when we're updating the values in the map too
	sortedKeys := getSortedKeysFromMap(&c.TempFiles)

	for _, val := range sortedKeys {
		areAllReduceJobsDone = areAllReduceJobsDone && (c.TempFiles[val] == DONE)
		if c.TempFiles[val] == NOT_STARTED || c.TempFiles[val] == TIMED_OUT {
			filePath = val
			break
		}
	}

	if filePath == "" {
		if areAllReduceJobsDone {
			/* logStr := fmt.Sprintf("[Coord] All the reduce jobs have been completed. c.TempFilesPath: %v", c.TempFiles)
			 * fmt.Println(logStr) */
			reply.Status = ALL_DONE

			/* if len(c.TempFiles) != len(c.TempFiles) {
			 *     log.Fatalf("The length of TempFiles and TempFiles doesn't match. c.TempFiles: %v, c.TempFiles: %v", c.TempFiles, c.TempFiles)
			 * } */

			// Set the flag that the whole map-reduce process has been done
			c.IsMapReduceDone = true
			return nil
		} else {
			/* logStr := fmt.Sprintf("[Coord] All the reduce jobs are in processing state. c.TempFiles: %v", c.TempFiles)
			 * fmt.Println(logStr) */
			reply.Status = WAIT_FOR_OTHERS
			return nil
		}
	}

	c.TempFiles[filePath] = PROCESSING

	reply.Id = c.CurrReduceJobID
	reply.Status = PROCESSING
	reply.NReduce = c.NReduce
	reply.NMap = c.NMap
	reply.Files = []string{filePath}

	c.CurrReduceJobID++

	/* fmt.Printf("got a request for ReduceJob. reply: %v\n", reply) */
	return nil
}

func (c *Coordinator) GetMapJob(req *CoordMapJobRequest, reply *CoordMapJobReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	fileName := ""
	areAllMapJobsDone := true

	// We need to get the sorted keys from the map because the order of iteration on a map is not guaranteed,
	// and especially so when we're updating the values in the map too
	sortedKeys := getSortedKeysFromMap(&c.FilesToProcess)

	for _, val := range sortedKeys {
		areAllMapJobsDone = areAllMapJobsDone && (c.FilesToProcess[val] == DONE)
		if c.FilesToProcess[val] == NOT_STARTED || c.FilesToProcess[val] == TIMED_OUT {
			fileName = val
			break
		}
	}

	if fileName == "" {
		if areAllMapJobsDone {
			/* logStr := fmt.Sprintf("[Coord] All the map jobs have been completed. c.TempFilesPath: %v", c.TempFiles)
			 * fmt.Println(logStr) */
			reply.Status = ALL_DONE

			if len(c.TempFiles) != len(c.FilesToProcess) {
				log.Fatalf("The length of TempFiles and FilesToProcess doesn't match. c.TempFiles: %v, c.FilesToProcess: %v", c.TempFiles, c.FilesToProcess)
			}
			return nil
		} else {
			/* logStr := fmt.Sprintf("[Coord] All the map jobs are in processing state. c.FilesToProcess: %v", c.FilesToProcess)
			 * fmt.Println(logStr) */
			reply.Status = WAIT_FOR_OTHERS
			return nil
		}
	}

	c.FilesToProcess[fileName] = PROCESSING

	reply.Id = c.CurrMapJobID
	reply.Status = PROCESSING
	reply.NReduce = c.NReduce
	reply.NMap = c.NMap
	reply.Files = []string{fileName}

	c.CurrMapJobID++

	/* fmt.Printf("got a request for MapJob. reply: %v\n", reply) */
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	return c.IsMapReduceDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	filesToProcess := make(map[string]int)

	for _, v := range files {
		filesToProcess[v] = NOT_STARTED
	}

	dir, _ := os.Getwd()
	dirPath := path.Join(dir, "intermediate-output")
	if _, err := os.Stat(dirPath); !os.IsNotExist(err) {
		err = os.RemoveAll(dirPath)
		if err != nil {
			log.Fatalf("Error while removing the intermediate output directory. dirPath: %v", dirPath)
		}
	}

	c := Coordinator{
		CurrMapJobID: 0, CurrReduceJobID: 0, Lock: &sync.Mutex{}, AllFiles: files, NReduce: nReduce,
		NMap: len(files), FilesToProcess: filesToProcess, TempFiles: make(map[string]int),
	}

	fmt.Printf("Starting the coordinator. nReduce: %v\n", nReduce)
	// Your code here.

	// Currently, each worker peforming a map task will take a single file
	/* pMap := len(files) */
	/* pMap := 2 * nReduce */

	c.server()
	return &c
}
