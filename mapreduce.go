package mapreduce

import (
	"log"
	"math"
	"sync"
)

type Selection uint8

const (
	Accepted Selection = 0
	Rejected           = 1
)

func Map(words <-chan string, smaps chan<- map[Selection]int, filter func(string) bool) {

	smap := map[Selection]int{}

	for word := range words {
		if filter(word) {
			smap[Accepted]++
		} else {
			smap[Rejected]++
		}
	}

	smaps <- smap

	close(smaps)
}

func Shuffle(inp []<-chan map[Selection]int, out [2]chan<- int) {

	var wg sync.WaitGroup
	wg.Add(len(inp))

	for _, i := range inp {
		go func(smaps <-chan map[Selection]int) {
			for smap := range smaps {
				accepted, ok := smap[Accepted]
				if ok {
					out[0] <- accepted
				}

				rejected, ok := smap[Rejected]
				if ok {
					out[1] <- rejected
				}
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(out[0])
		close(out[1])
	}()
}

func Reduce(in <-chan int, out chan<- float64) {

	cnt := 0
	sum := 0

	for i := range in {
		sum += i
		cnt++
	}

	mean := float64(sum) / float64(cnt)

	out <- math.Floor(mean*100) / 100

	close(out)
}

func ReadWords(words []string, c chan<- string) {
	for _, word := range words {
		c <- word
	}
	close(c)
}

func GetResult(inp []<-chan float64) float64 {
	var wg sync.WaitGroup
	wg.Add(len(inp))

	var res float64

	for i := 0; i < len(inp); i++ {
		go func(cnt int, ch <-chan float64) {
			val := <-ch
			state := Selection(cnt)

			switch state {
			case Accepted:
				log.Println("Acceptance mean: ", val)
				res = val
			case Rejected:
				log.Println("Rejection mean: ", val)
				break
			}

			wg.Done()
		}(i, inp[i])
	}

	wg.Wait()

	return res
}

func MapReduce(input [][]string, filter func(string) bool) float64 {
	smaps := []<-chan map[Selection]int{}

	for _, inp := range input {
		words := make(chan string)
		go ReadWords(inp, words)

		smap := make(chan map[Selection]int)
		go Map(words, smap, filter)

		smaps = append(smaps, smap)
	}

	accepted := make(chan int)
	rejected := make(chan int)
	go Shuffle(smaps, [2]chan<- int{accepted, rejected})

	r1 := make(chan float64)
	r2 := make(chan float64)
	go Reduce(accepted, r1)
	go Reduce(rejected, r2)

	return GetResult([]<-chan float64{r1, r2})
}
