package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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
		kvs := make([]KeyValue, 0)
		if resp.TaskInfo.Type == kMapTask {
			for _, fileName := range resp.TaskInfo.FileNames {
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("[Worker] OpenFile failed: %v | %v | %v", err, fileName, resp.TaskInfo)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					_ = file.Close()
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
						_ = file.Close()
						log.Fatalf("[Worker] Encode failed: %v | %v | %v | %v | %v", err, fileName, resp.TaskInfo, idx, kv)
					}
				}
				_ = file.Close()
			}
		} else if resp.TaskInfo.Type == kReduceTask {
			for _, fileName := range resp.TaskInfo.FileNames {
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("[Worker] OpenFile failed: %v | %v | %v", err, fileName, resp.TaskInfo)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kvs = append(kvs, kv)
				}
			}
			sort.Slice(kvs, func(i, j int) bool {
				return kvs[i].Key < kvs[j].Key
			})
			outputFileName := fmt.Sprintf("mr-out-%v", resp.TaskInfo.ID)
			outputFile, err := os.Create(outputFileName)
			if err != nil {
				log.Fatalf("[Worker] OpenFile failed: %v | %v | %v", err, outputFileName, resp.TaskInfo)
			}
			i := 0
			for i < len(kvs) {
				j := i + 1
				for j < len(kvs) && kvs[j].Key == kvs[i].Key {
					j++
				}
				values := make([]string, 0, j-i)
				for k := i; k < j; k++ {
					values = append(values, kvs[k].Value)
				}
				output := reducef(kvs[i].Key, values)
				_, _ = fmt.Fprintf(outputFile, "%v %v\n", kvs[i].Key, output)
				i = j
			}
			_ = outputFile.Close()
		}
		setTaskFinishedReq := &SetTaskFinishedReq{
			TaskInfo: resp.TaskInfo,
		}
		setTaskFinishedResp := &SetTaskFinishedResp{}
		_ = call("Master.SetTaskFinished", setTaskFinishedReq, setTaskFinishedResp)
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
