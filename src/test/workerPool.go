package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Job struct {
	id int
	randno int
}

type Result struct {
	job Job
	sum int
}

func digits(number int) int {
	sum := 0
	no := number
	for no != 0 {
		digit := no % 10
		sum += digit
		no /= 10
	}
	time.Sleep(1 * time.Second)
	return sum
}

func worker(jobs chan Job, results chan Result, wg *sync.WaitGroup)  {
	for job := range jobs {
		output := Result{
			job: job,
			sum: digits(job.randno),
		}
		results <- output
	}

	wg.Done()
}

func createWorkerPool(jobs chan Job, results chan Result, noOfWorkers int)  {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go worker(jobs, results, &wg)
	}

	wg.Wait()
	close(results)
}

func printResults(results chan Result, wg *sync.WaitGroup)  {
	for v := range results {
		fmt.Printf("Job: %v, sum: %d\n", v.job, v.sum)
	}
	wg.Done()
}

func createJobs(noOfJobs int, jobs chan Job) {
	for i := 0; i < noOfJobs; i++ {
		//if (i % 25 == 0) {
		//	time.Sleep(2 * time.Second)
		//}

		jobs <- Job{
			id:    i,
			randno: rand.Intn(10000),
		}
	}

	close(jobs)
}

func mmain()  {
	startTime := time.Now()

	var wg sync.WaitGroup

	// Used to listen to new jobs
	var jobs = make(chan Job, 10)

	// Used to store the results
	var results = make(chan Result, 10)

	wg.Add(1)
	go createJobs(10000, jobs)
	go createWorkerPool(jobs, results, 1000)

	go printResults(results, &wg)
	wg.Wait()

	diff := time.Now().Sub(startTime)
	fmt.Println("total time taken ", diff.Seconds(), "seconds")
}