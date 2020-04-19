package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		req := &GetTaskReq{}
		resp := &GetTaskResp{}
		result := call("Master.GetTask", req, resp)
		if result == false || resp.StatusCode == kStatusCodeFinish {
			break
		}
		if resp.StatusCode == kStatusCodeNoTask {
			time.Sleep(time.Second)
			continue
		}
		if resp.TaskInfo.Type == kMapTask {
			kvs := make([]KeyValue, 0)
			for _, fileName := range resp.TaskInfo.FileNames {
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("[Worker] OpenFile failed: %v | %v | %v", err, fileName, resp.TaskInfo)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("[Worker] Read from file failed: %v | %v | %v", err, fileName, resp.TaskInfo)
				}
				_ = file.Close()
				kvsFromFile := mapf(fileName, string(content))
				kvs = append(kvs, kvsFromFile...)
			}
			reduceKVsMap := make(map[int][]KeyValue)
			for idx := 0; idx < resp.TotalReduceTaskCount; idx++ {
				reduceKVsMap[idx] = make([]KeyValue, 0)
			}
			for _, kv := range kvs {
				idx := ihash(kv.Key) % resp.TotalReduceTaskCount
				reduceKVsMap[idx] = append(reduceKVsMap[idx], kv)
			}
			for idx := 0; idx < resp.TotalReduceTaskCount; idx++ {
				fileName := fmt.Sprintf("mr-%v-%v", resp.TaskInfo.ID, idx)
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("[Worker] OpenFile failed: %v | %v | %v", err, fileName, resp.TaskInfo)
				}
				enc := json.NewEncoder(file)
				for _, kv := range reduceKVsMap[idx] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("[Worker] Encode failed: %v | %v | %v | %v | %v", err, fileName, resp.TaskInfo, idx, kv)
					}
				}
			}
		} else if resp.TaskInfo.Type == kReduceTask {

		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
