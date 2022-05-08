package main

import (
	"fmt"
	"time"
)

func pinger(c chan string) {
	for i := 0; i < 3; i++ {
		c <- "ping"
		time.Sleep(time.Second)
	}
	/* close(c) */
}

func main() {
	var c chan string = make(chan string)

	go pinger(c)

	for msg := range c {
		fmt.Println(msg)
	}
}
