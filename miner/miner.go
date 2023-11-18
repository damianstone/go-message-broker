package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/rpc"
	"time"
	"uk.ac.bris.cs/solutions/distributed3/pairbroker/stubs"
)

/*
	the miner is just a server that send the numbers to multiply to the broker server,
	so basically it is a client of the broker that publish these number so then the
 	servers subscribed to the "multiply" topic will receive the numbers (as requests) to multiply them
*/

func main() {
	brokerAddr := flag.String("broker", "127.0.0.1:8030", "Address of broker instance")
	flag.Parse()
	//Dial broker address.
	client, _ := rpc.Dial("tcp", *brokerAddr)
	status := new(stubs.StatusReport)
	//Create a new buffered channel
	client.Call(stubs.CreateChannel, stubs.ChannelRequest{Topic: "multiply", Buffer: 10}, status)
	//Random seed for 'mining' work.
	rand.Seed(time.Now().UnixNano())
	for {
		//Create two new random integers
		newpair := stubs.Pair{rand.Intn(999999999), rand.Intn(999999999)}

		//Form a request to publish it in 'multiply'
		towork := stubs.PublishRequest{Topic: "multiply", Pair: newpair}

		//Call the broker
		err := client.Call(stubs.Publish, towork, status)
		if err != nil {
			fmt.Println("RPC client returned error:")
			fmt.Println(err)
			fmt.Println("Shutting down miner.")
			break
		}

		time.Sleep(1 * time.Second)
	}
}
