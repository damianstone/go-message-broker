package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"uk.ac.bris.cs/solutions/distributed3/pairbroker/stubs"
)

/*
	factory is a server connected to the broker server.
	This server what it does is just subscribe the existing channels (topics) to do whatever
	calculation with the published numbers from the miner server

	So:
	1- Connects to the broker server as a client
	2- Register the factory functions and create a new listener server
	3- Subscribe to a topic (channel) from the broker
	4- When subscribed passed the factory function (endpoint) as a callback to the broker,
		so then the broker can call the factory functions from the broker server
*/

var (
	mulch = make(chan int, 2)
)

type Factory struct{}

func makedivision(ch chan int, client *rpc.Client) {
	for {
		x := <-ch
		y := <-ch
		newpair := stubs.Pair{X: x, Y: y}
		towork := stubs.PublishRequest{Topic: "divide", Pair: newpair}
		status := new(stubs.StatusReport)
		err := client.Call(stubs.Publish, towork, status)
		if err != nil {
			fmt.Println("RPC client returned error:")
			fmt.Println(err)
			fmt.Println("Dropping division.")
		}
	}
}

func (f *Factory) Multiply(req stubs.Pair, res *stubs.JobReport) (err error) {
	res.Result = req.X * req.Y
	fmt.Println(req.X, "*", req.Y, "=", res.Result)
	mulch <- res.Result
	return
}

func (f *Factory) Divide(req stubs.Pair, res *stubs.JobReport) (err error) {
	res.Result = req.X / req.Y
	fmt.Println(req.X, "/", req.Y, "=", res.Result)
	return
}

func main() {
	// RPC server that registers a multiply procedure
	pAddr := flag.String("ip", "127.0.0.1:8050", "IP and port to listen on")

	// Connects to the broker as an RPC client
	brokerAddr := flag.String("broker", "127.0.0.1:8030", "Address of broker instance")
	flag.Parse()
	client, cErr := rpc.Dial("tcp", *brokerAddr)
	if cErr != nil {
		fmt.Println(cErr)
	} else {
		fmt.Println("Connected to broker at " + *brokerAddr)
	}

	status := new(stubs.StatusReport)

	client.Call(stubs.CreateChannel, stubs.ChannelRequest{Topic: "divide", Buffer: 10}, status)

	// register factory as an RPC server
	rpc.Register(&Factory{})
	listener, lisError := net.Listen("tcp", *pAddr)
	if lisError != nil {
		fmt.Println(lisError)
	} else {
		fmt.Println("Listening on " + *pAddr)
	}

	// subscribe to multiply
	request1 := stubs.Subscription{
		Topic:          "multiply",
		FactoryAddress: *pAddr,
		Callback:       "Factory.Multiply",
	}
	client.Call(stubs.Subscribe, request1, status)
	fmt.Println("Response: " + status.Message)

	// subscribe to divide
	request2 := stubs.Subscription{
		Topic:          "divide",
		FactoryAddress: *pAddr,
		Callback:       "Factory.Divide",
	}

	client.Call(stubs.Subscribe, request2, status)
	go makedivision(mulch, client)
	defer listener.Close()
	rpc.Accept(listener)
	flag.Parse()
}
