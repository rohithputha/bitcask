package main

import (
	"bitcask/cask"
	"fmt"
)

func main() {
	c := cask.NewCaskDb()
	//go get(c)
	//go put(c)
	for i := 0; i < 1000; i++ {
		c.Put("key1", "4366")

		c.Put("key2", "value2")

		c.Put("key3", []string{"key3", "Rohith"})

		c.Put("key4", "value4")

		fmt.Println(i)
	}
	//cask.TestFileCreation()
	//cask.TestKeyDirPut()
	//time.Sleep(1 * time.Second)
	c.Put("key1", "4366")

	//time.Sleep(15 * time.Second)
	fmt.Println(c.Get("key1"))
	//fmt.Println(c.Get("key3"))
}
func get(c *cask.CaskDb) {
	for i := 0; i < 5000; i++ {
		fmt.Println("hello")
		fmt.Println(c.Get("key1"))
		fmt.Println(c.Get("key3"))
	}
}
func put(c *cask.CaskDb) {
	//for i := 0; i < 5000; i++ {
	//	c.Put("key1", "4366")
	//
	//	c.Put("key2", "value2")
	//
	//	c.Put("key3", []string{"key3", "Rohith"})
	//
	//	c.Put("key4", "value4")
	//
	//}
}
