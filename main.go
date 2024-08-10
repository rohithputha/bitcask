package main

import (
	"bitcask/cask"
	"time"

	"fmt"
)


func main() {
	c := cask.NewCaskDb()
	go get(c)
	for i:=0;i<40;i++{
		c.Put("key1", "4366")
	
		c.Put("key2", "value2")
	
		c.Put("key3", []string{"key3","value4"})
		
		c.Put("key4", "value4")
		
	}
	time.Sleep(2*time.Second)	
	fmt.Println(c.Get("key1"))
	fmt.Println(c.Get("key3"))
}
func get(c *cask.CaskDb){
	for i:=0;i<40;i++{
		fmt.Println(c.Get("key1"))
	fmt.Println(c.Get("key3"))
	}
}

