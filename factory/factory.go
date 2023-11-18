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

type Factory struct{}

// This is just a helper function that attempts to determine this
// process' IP address.
func getOutboundIP() string {
	conn, _ := net.Dial("udp", "8.8.8.8:80")
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr).IP.String()
	return localAddr
}

func (f *Factory) Multiply(req stubs.Pair, res *stubs.JobReport) (err error) {
	res.Result = req.X * req.Y
	fmt.Println(req.X, "*", req.Y, "=", res.Result)
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

	// register factory as an RPC server
	rpc.Register(&Factory{})
	listener, lisError := net.Listen("tcp", *pAddr)
	if lisError != nil {
		fmt.Println(lisError)
	} else {
		fmt.Println("Listening on " + *pAddr)
	}

	fmt.Println(*pAddr)
	// send subscription request to broker
	request := stubs.Subscription{
		Topic:          "multiply",
		FactoryAddress: *pAddr,
		Callback:       "Factory.Multiply",
	}
	client.Call(stubs.Subscribe, request, status)
	fmt.Println("Response: " + status.Message)

	defer listener.Close()
	rpc.Accept(listener)
	flag.Parse()
}
