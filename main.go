package main

import (
	"net/http"
	"sync"
	"sort"
	"fmt"
	"strconv"
	"github.com/spaolacci/murmur3"
	"github.com/gin-gonic/gin"
)

const (
	minReplicas = 3
	maxReplicas = 10
	maxWeight = 100
	bucketSize = 3
)

type ConsistentHash struct{
 hashFunc func(data []byte) uint64	
 virtualNodes [] uint64 //List of virtual node keys
 ring map[uint64][]interface {}   //hashmap to map hashed virtual node keys to the node
 nodeWeights map[string]int //keep weight of each node bucket
 nodes map[string]bool //Hash map to keep track of nodes
 mutex sync.RWMutex
}

/** Functions to implement
*
* 1. AddNode - Adds a virtual node
* 2. AddWithReplicas 
* 3. AddWithWeight
* 4. Remove a node
* 5. Get
*/

func newConsistentHash() *ConsistentHash{
	return &ConsistentHash{
		hashFunc : murmur3.Sum64,
		nodeWeights: make(map[string]int),
		ring:    make(map[uint64][]any),
		nodes:    map[string]bool{},
	}
}

func (ch *ConsistentHash) AddWithWeight (node string, weight int){
	ch.mutex.Lock()
	defer ch.mutex.Unlock()
	present := ch.nodes[node]
	fmt.Println(ch.nodes[node], "node")
	if present == false{
		if ch.nodeWeights[node] == weight{
			fmt.Println("Node weight is the same, no update")
			return
		}
		ch.nodes[node] = true
		ch.nodeWeights[node] = minReplicas * weight/maxWeight
	} else {
		ch.updateWeight(node, weight)
	}
	ch.rebuild()
	
}

func (ch *ConsistentHash) updateWeight(node string, weight int){

}

func (ch *ConsistentHash) AddNode(node string){
     ch.mutex.Lock()
	 defer ch.mutex.Unlock()
	 ch.nodes[node] = true
	 ch.nodeWeights[node] = minReplicas
	 ch.rebuild()
}

func (ch *ConsistentHash) rebuild() {
    

	fmt.Println("node weight values------")
    for key,values := range ch.nodeWeights{
		fmt.Println(key, "-", values)
	}

	// Clear the current ring
	ch.ring = make(map[uint64][]any)
	fmt.Println("Old ring values")
	for key, values := range ch.ring {
		fmt.Println(key, "-", values)
	}
	// Create a hashmap index for virtualNodes
	index := make(map[uint64]bool)

	// Populate the index from existing virtualNodes
	for _, key := range ch.virtualNodes {
		index[key] = true
	}

	// Recreate the ring with updated virtual nodes
	for node := range ch.nodes {
		fmt.Println(node, ch.nodeWeights[node], "inside rebuild")
		for i := 0; i < ch.nodeWeights[node]; i++ {
			key := ch.hashFunc([]byte(node + strconv.Itoa(i)))
			if _, ok := index[key]; !ok {
				// Append to virtualNodes only if key is not present in the index
				ch.virtualNodes = append(ch.virtualNodes, key)
				index[key] = true
			}
			if _, ok := ch.ring[key]; !ok {
				ch.ring[key] =  make([]interface{}, 0)
			ch.ring[key] = append(ch.ring[key], node)
		}
	}
}

	sort.Slice(ch.virtualNodes, func(i, j int) bool {
		return ch.virtualNodes[i] < ch.virtualNodes[j]
	})

	// Transfer the existing items from the old ring to the new ring
	
	// Print the updated state of the ring
	fmt.Println("Rebuilt Ring:")
	for _, key := range ch.virtualNodes {
		fmt.Println(key, "-", ch.ring[key])
	}
	fmt.Println("-----")

}

func (ch *ConsistentHash)GetNode(node string) string{
	ch.mutex.RLock()
	defer ch.mutex.RUnlock()
   
	key := ch.hashFunc([]byte(node))
	//sort and search the index of the first virtual node greater than the 
	index := sort.Search(len(ch.virtualNodes), func(i int) bool {
		return ch.virtualNodes[i] >= key
	}) % len(ch.virtualNodes)
    
	res := ch.ring[ch.virtualNodes[index]]
	return res[0].(string)
}

func (ch *ConsistentHash)DeleteVirtualNodes(node string){
	weight := ch.nodeWeights[node]
	for i:= 0;i<weight;i++{
		key := ch.hashFunc([]byte(node + strconv.Itoa(i)))
		for i,k := range ch.virtualNodes{
			if k == key {
				ch.virtualNodes = append(ch.virtualNodes[:i],ch.virtualNodes[i+1:]...)
				delete(ch.ring,key)
				break
			}
		}

	}
	for _, key := range ch.virtualNodes {
		fmt.Println(key, "-", ch.ring[key])
	}
	fmt.Println("Ring:")
	for key, values := range ch.ring {
		fmt.Println(key, "-", values)
	}
	
}

func (ch *ConsistentHash)DeleteNode (node string){
	ch.mutex.Lock()
	defer ch.mutex.Unlock()
	ch.DeleteVirtualNodes(node)
	delete(ch.nodes,node)
	fmt.Println("-----")
	for key, _:= range ch.nodes {
		fmt.Println(key, "-", ch.nodes[key])
	}
	fmt.Println("-----")
}

func main() {
	ch :=  newConsistentHash()
	for i :=0 ; i< bucketSize; i++{
		ch.AddNode("localhost:" + strconv.Itoa(i))
	}
	//res := ch.GetNode("localhost:" + strconv.Itoa(1))
	//ch.DeleteNode("localhost:" + strconv.Itoa(1))
	ch.AddWithWeight("localhost:" + strconv.Itoa(4), 40)
	//fmt.Println(res, "result")
	
	router := gin.Default()
    router.GET("/hello", hello)
    router.Run("localhost:8080")
}

func hello(c *gin.Context){
	c.IndentedJSON(http.StatusOK, gin.H{
		"message": "Hello",
	})
}