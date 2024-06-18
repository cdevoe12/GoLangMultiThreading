package main

import "fmt"
import "os"
import "strconv"
import "time"
import "encoding/binary"
import "math/rand"
import "sync"
import "math/big"
import "log/slog"

type job struct {
	start int
	length int
	path string
}

type result struct{
	jobDes job
	numOfPrimes int
}

//DOES NOT READ FILE, only dispatches it to the queue for the workers
//Dispatch is to create the jobs, send to channel for worker function
func dispatch(jobs chan job, path string, N int, wait *sync.WaitGroup){
	
	//dat, _ := os.ReadFile(path)
	//fmt.Println(dat)
	
	file, error := os.Open(path)
	if error != nil{
		fmt.Println("Error")
		}
		
	//Gets data of file
	stats, _ := file.Stat()

	start := 0
	length := (N)
	
	//Get the segment sizes
	segs := stats.Size()/(int64(N))
	//fmt.Println("segs",segs)
	
	//Runs loop to get the starting poinrt and length if needed
	for i :=0; int64(i) < segs; i++{
		
		start = (N) * i
		
		if segs - 1 == int64(i) {
			length = int(stats.Size()) - start
		}
		//fmt.Println(length)
		jobs <- job{start, length, path}
	}
	
	//Closes file, closes job once all jobs are made, tells the sync that it is done
	file.Close()
	close(jobs)
	wait.Done()
		
}


//This is our worker, it will grab jobs from the job channel and then send results to results channel
func workers(jobs chan job, results chan result, wait *sync.WaitGroup, C int, num int){

	//fmt.Println("Worker #", num)
	primeCount := 0
	
	//Waits for a random time
	time.Sleep(time.Duration(rand.Intn(200)+400) * time.Millisecond)
	
	
	//Grabs the current job as long as there are jobs in the channel
	for current_job := range jobs {
		//fmt.Println(test.start)
		
		file, error := os.Open(current_job.path)
		if error != nil{
			fmt.Println("Error")
			}
			
		file.Seek(int64(current_job.start),0)
		//fmt.Println("THIS IS OUR TEST:"])
		
		bytes := make([]byte, C)
		
		//fmt.Println("Bytes:",bytes)
		
		//Loops through the file collect a byte size of C portion
		for j := 0; j < current_job.length/1024; j++ {
			size, error := file.Read(bytes)
			
			//Breaks out of the code when there is nothing left to read
			if error != nil{
				break
			}
			
			//Breaks the C byte size down into smaller bits to run against the prime equation
			for i := 0; i < size/8; i++{
				data := binary.LittleEndian.Uint64(bytes[i * 8 : (i + 1) * 8])
				//fmt.Println(data)
				
				var newData big.Int
				
				//fmt.Println(newData.SetUint64(data))
				
				flag := (newData.SetUint64(data)).ProbablyPrime(20)
				//fmt.Println(flag)
				
				if flag{
					primeCount = primeCount + 1
				}
				
			}
			//fmt.Println(current_job.start, num, j)
			//fmt.Println("TEST2",bytes[:size])
			//break
		}
		
		//Closes file, sends results to channel, and logs info
		file.Close()
		results <- result{current_job,primeCount}
		slog.Info("Worker data", "Path",current_job.path, "Start", current_job.start, "Length", current_job.length, "Prime Count", primeCount)
		primeCount = 0
		time.Sleep(time.Duration(rand.Intn(200)+400) * time.Millisecond)
	}
	
	wait.Done()
	//fmt.Println("End")
}

//Consolidates all of the data sent to results, once the channel is empty it will close.
//This function will only finish once all others are done
func consolidator (results chan result, wait *sync.WaitGroup, finalResult chan int){
	totalPrime := 0
	//fmt.Println("Consolidator")
	
	for current_result := range results {
		totalPrime = current_result.numOfPrimes + totalPrime
		//fmt.Println("C Prime:",totalPrime)
	}
	
	//fmt.Println("Total prime is =",totalPrime)
	finalResult <- totalPrime
	close(finalResult)
	wait.Done()
}


func main() {
    
	//creates wait group
	var wait sync.WaitGroup
	
	//creates start time for time of main
	start := time.Now()
	
	//Creats channels and M N And C
	jobs := make(chan job)
	results := make (chan result)
	M := 0
	N := 0
	C := 0
	
	//gains passed arguments
	input := os.Args[1:]
	//fmt.Println(input)
	
	if len(os.Args) <= 1 {
		fmt.Println("ERROR NO PATH")
		os.Exit(1)
	}
	
	//gets path
	path := input[0]
	
	//fmt.Println(len(os.Args))
	
	//sets M
	if len(os.Args) > 2{
		M, _ = strconv.Atoi(input[1])
	} else {
		M = 1
	}
	
	//Sets N
	if len(os.Args) > 3{
		N, _ = strconv.Atoi(input[2])
	} else {
		N = 64 * 1024
	}
	
	//Sets C
	if len(os.Args) > 4{
		C, _ = strconv.Atoi(input[3])
	} else {
		C = 1024
	}
		
	//fmt.Println(path)
	//fmt.Println(M)
	//fmt.Println(N)
	//fmt.Println(C)
	
	//Runs dispatch and waits
	wait.Add(1)
	go dispatch(jobs, path, N, &wait)
	
	//Runs M workers and waits
	for i:=0; i < M; i++{
		//fmt.Println(i)
		wait.Add(1)
		go workers(jobs,results, &wait, C, i)
		}
		
	//wait.Add(1)
	
	//creates a channel called final results and starts consoildator
	finalResult := make(chan int)
	go consolidator(results,&wait, finalResult)
	
	//waits for Workers and Dispatcher to finish
	wait.Wait()
	
	//waits for consoildator
	wait.Add(1)
	
	close(results)
	primeCount := <- finalResult
	wait.Wait()
	elapsed := time.Since(start)
	fmt.Println("Main took", elapsed)
	fmt.Println("The final count of prime numbers is", primeCount)
}