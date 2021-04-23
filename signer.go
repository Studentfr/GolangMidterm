package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)


const TH = 6


func ExecutePipeline(jobs ...job)  {
	in := make(chan interface{})
	wg := &sync.WaitGroup{}
	for _, jobb := range jobs{
		wg.Add(1)
		out := make(chan interface{})
		go func(in, out chan interface{}, wg *sync.WaitGroup, jobb job) {
			defer wg.Done()
			defer close(out)
			jobb(in, out)
		}(in, out, wg, jobb)
		in = out

	}
	wg.Wait()

}

func SingleHash(in chan interface{}, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mut := &sync.Mutex{}
	for data := range in{
		wg.Add(1)
		go SingleHashWorker(data, out, wg, mut)
	}
	wg.Wait()
}

func SingleHashWorker(data interface{}, out chan interface{}, wg *sync.WaitGroup, mut *sync.Mutex) {
	defer wg.Done()
	convertedData := strconv.Itoa(data.(int))
	mut.Lock()
	md5Hash := DataSignerMd5(convertedData)
	mut.Unlock()
	ch := make(chan string)
	go func(ch chan string) {
		crc32 := DataSignerCrc32(convertedData)
		ch <- crc32
	}(ch)
	crc32Md5 := DataSignerCrc32(md5Hash)
	crc32 := <- ch
	out <- crc32 + "~" + crc32Md5
}

func MultiHash(in chan interface{}, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for data := range in{
		wg.Add(1)
		go MultiHashWorker(data, out, wg)
	}
	wg.Wait()
}

func MultiHashWorker(data interface{}, out chan interface{}, wg *sync.WaitGroup)  {
	defer wg.Done()
	convertedData := data.(string)
	fmt.Println(convertedData)
	array := make([]string, TH)
	wait := &sync.WaitGroup{}
	for i := 0; i < TH; i++ {
		wait.Add(1)
		go func(i int, array []string, wait *sync.WaitGroup, convertedData string) {
			defer wait.Done()
			newData := DataSignerCrc32(strconv.Itoa(i) + convertedData)
			array[i] = newData
		}(i, array, wait, convertedData)
	}
	wait.Wait()
	out <- strings.Join(array, "")

}

func CombineResults(in chan interface{}, out chan interface{}) {
	var dataArray []string

	for data := range in {
		convertedData := data.(string)
		dataArray = append(dataArray, convertedData)
	}
	sort.Strings(dataArray)
	out <- strings.Join(dataArray, "_")
}