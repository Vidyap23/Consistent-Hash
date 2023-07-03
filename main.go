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
	bucketSize = 3
)

type ConsistentHash struct{
 hashFunc func(data []byte) uint64	
 replicas int //Number of virtual nodes
 virtualNodes [] uint64 //List of virtual node keys
 ring map[uint64][]interface {}   //hashmap to map hashed virtual node keys to the node
//  minWeight int64 
//  maxWeight int64
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

func newConsistentHash(replicas int) *ConsistentHash{
	if replicas < minReplicas{
		replicas = minReplicas
	}
	return &ConsistentHash{
		hashFunc : murmur3.Sum64,
		replicas : replicas,
		ring:    make(map[uint64][]any),
		nodes:    map[string]bool{},
	}

}

func (ch *ConsistentHash) AddNode(node string){
     ch.mutex.Lock()
	 defer ch.mutex.Unlock()
	 ch.nodes[node] = true
	 for i:= 0 ; i < ch.replicas; i++ {
		key := ch.hashFunc([]byte(node + strconv.Itoa(i)))
		ch.virtualNodes = append(ch.virtualNodes, key)

		//This ensures that each key in the ring map has a unique set of values.
		if _, ok := ch.ring[key]; !ok {
			ch.ring[key] = make([]interface{}, 0)
		}
		ch.ring[key] = append(ch.ring[key], node)
	 }
	 sort.Slice(ch.virtualNodes, func(i, j int) bool {
		return ch.virtualNodes[i] < ch.virtualNodes[j]
	})
	for _, key := range ch.virtualNodes {
		fmt.Println(key, "-", ch.ring[key])
	}
	fmt.Println("Ring:")
	for key, values := range ch.ring {
		fmt.Println(key, "-", values)
	}
	fmt.Println("-----")
	for key, _:= range ch.nodes {
		fmt.Println(key, "-", ch.nodes[key])
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
	for i:= 0;i<ch.replicas;i++{
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
	ch :=  newConsistentHash(minReplicas)
	for i :=0 ; i< bucketSize; i++{
		ch.AddNode("localhost:" + strconv.Itoa(i))
	}
	//res := ch.GetNode("localhost:" + strconv.Itoa(1))
	ch.DeleteNode("localhost:" + strconv.Itoa(1))
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