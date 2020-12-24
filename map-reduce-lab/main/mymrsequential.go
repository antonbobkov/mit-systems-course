package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "../mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "hash/fnv"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type MapFunc func(string, string) []mr.KeyValue
type ReduceFunc func(string, []string) string

type Task struct {
	is_map_task bool
	
	map_file string
	map_task_id int

	reduce_task_id int
	
	map_num int
	reduce_num int
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func ExecuteMapTask(t Task, mapf MapFunc) {
	file, err := os.Open(t.map_file)
	if err != nil {
		log.Fatalf("cannot open %v", t.map_file)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", t.map_file)
	}
	file.Close()
	kva := mapf(t.map_file, string(content))

	file_handles := make([]*os.File, 0)
	
	for b := 0; b < t.reduce_num; b++ {
		out_file_name := fmt.Sprintf("intermediate-map-%v-bucket-%v.txt", t.map_task_id, b)
		ofile, _ := os.Create(out_file_name)
		file_handles = append(file_handles, ofile)
	}

	for _, kv := range kva {
		b := ihash(kv.Key) % t.reduce_num
		fmt.Fprintf(file_handles[b], "%v %v\n", kv.Key, kv.Value)		
	}

	for _, ofile := range(file_handles) {
		ofile.Close()
	}
}

func ReadKVPairs(file_name) []mr.KeyValue {
	
}

func ExecuteReduceTask(t Task, reducef ReduceFunc) {
	intermediate := []mr.KeyValue{}
	for m := 0; m < t.map_num; m++ {
		in_file_name := fmt.Sprintf("intermediate-map-%v-bucket-%v.txt", m, t.reduce_task_id)

		// TODO: read kv pair

		intermediate = append(intermediate, kv)

		sort.Sort(ByKey(intermediate))

		oname := fmt.Sprintf("mr-out-%v", t.reduce_task_id)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])
	reduce_num := 3
	map_num := len(os.Args[2:])
		
	for i, filename := range os.Args[2:] {
		t := Task{}
		t.is_map_task = true
		t.map_file = filename
		t.map_task_id = i
		t.reduce_num = reduce_num
		
		ExecuteMapTask(t, mapf)
	}

	for i := 0; i < reduce_num; i++ {
		t := Task{}
		t.is_map_task = false
		t.map_num = map_num

		ExecuteReduceTask(t, reducef)
  }
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
