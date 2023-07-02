package main

import (
	"net/http"
	"sync"
	"github.com/gin-gonic/gin"
)

//data structure for the hash ring
type HashRing struct{
	hashFunc //for the key to be hashed
	replicas int //Number of virtual nodes for each node
	keys   []uint64 //sorted hashkeys
	ring map[uint64][] any
	nodes  map[string]lang.PlaceholderType
	hashMap map[int]string //Map the hash key to node
	mutex sync.RWMutex //locking while adding/deleting nodes - thread safety
}
//Add node to hash ring

//Remove node from hash ring


//Get a node

//Remove a key
func main() {
	router := gin.Default()
    router.GET("/hello", hello)
    router.Run("localhost:8080")
}

func hello(c *gin.Context){
	c.IndentedJSON(http.StatusOK, gin.H{
		"message": "Hello",
	})
}