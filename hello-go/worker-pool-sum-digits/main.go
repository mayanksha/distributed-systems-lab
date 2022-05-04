package main

import (
	"fmt"
	"math/rand"
	"sync"
)

const (
	N_NUMS      = 10_000_000
	MAX_WORKERS = 100
)

type Job struct {
	id   int   // ID of the job
	nums []int // numbers which need to be processed
}

type Result struct {
	job Job
	res int
}

type Context struct {
	wg      *sync.WaitGroup
	resChan chan Result
}

func processJob(job Job, ctx *Context) {
	defer ctx.wg.Done()

	/* fmt.Printf("Current jobID: %d. Nums: %v\n", job.id, job.nums) */

	sum := 0
	for _, num := range job.nums {
		for num > 0 {
			digit := num % 10
			sum += digit
			num = num / 10

			/* fmt.Printf("Current jobID: %d. num: %d, digit: %d\n", job.id, num, digit) */
		}

		// Simulate some activity which takes 1 sec
		/* time.Sleep(time.Second) */
	}

	ctx.resChan <- Result{job, sum}
}

func initAndAllocateJobs(randNums []int) {
	numsPerWorker := N_NUMS / MAX_WORKERS

	var wg sync.WaitGroup
	var resChan chan Result = make(chan Result, N_NUMS)

	ctx := Context{&wg, resChan}
	for i := 0; i <= MAX_WORKERS; i++ {
		wg.Add(1)

		var currNums []int
		for k := 0; k < numsPerWorker; k++ {
			indexToAccess := i*numsPerWorker + k
			if indexToAccess >= N_NUMS {
				break
			}

			currNums = append(currNums, randNums[indexToAccess])
		}

		go processJob(Job{i, currNums}, &ctx)
	}

	wg.Wait()
	close(resChan)

	sumAll := 0
	for result := range resChan {
		/* fmt.Printf("Result => ans: %d, jobID: %d. nums: %v\n", result.res, result.job.id, result.job.nums) */
		sumAll += result.res
	}

	fmt.Println("Sum of all the digits: ", sumAll)
}

func main() {
	var nums []int
	n := N_NUMS
	for i := 0; i < n; i++ {
		/* nums = append(nums, i) */
		nums = append(nums, rand.Int())
	}

	/* fmt.Println("Nums: ", nums) */
	initAndAllocateJobs(nums)
}
