package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"uk.ac.bris.cs/solutions/distributed3/pairbroker/stubs"
)

/*
	The broker acts as a server in between the factory and miner servers.
	It does that using a subscription - publish model

	How it works:
	1- The broker is up as a listener server
	2- any other server can connect to it as a client and start creating topics (channels)
		topics in this case are just the data structure that is being used to store the data and
		pass it to the subscribers
	3- a subscriber server is a client and listener server at the same time that stores the logic of what to do
		with the data that is being passed to it from the broker
	4- When a client publish server publish something to a topic, the broker will pass it to the subscriber server
	5- The subscriber server will do whatever it wants with the data and send the result back to the broker.
*/

var topics = make(map[string]chan stubs.Pair)
var topicmx sync.RWMutex

// Create a new topic as a buffered channel.
func createTopic(topic string, buffer int) {
	topicmx.Lock()
	defer topicmx.Unlock()
	if _, ok := topics[topic]; !ok {
		topics[topic] = make(chan stubs.Pair, buffer)
		fmt.Println("Created channel #", topic)
	}
}

// The Pair is published to the topic.d
func publish(topic string, pair stubs.Pair) (err error) {
	topicmx.RLock()
	defer topicmx.RUnlock()
	if ch, ok := topics[topic]; ok {
		ch <- pair
	} else {
		return errors.New("No such topic.")
	}
	return
}

// The subscriber loops run asynchronously, reading from the topic and sending the err
// 'job' pairs to their associated subscriber.
func subscriberLoop(topic chan stubs.Pair, client *rpc.Client, callback string) {
	for {
		job := <-topic
		response := new(stubs.JobReport)
		err := client.Call(callback, job, response)
		if err != nil {
			fmt.Println("Error")
			fmt.Println(err)
			fmt.Println("Closing subscriber thread.")
			//Place the unfulfilled job back on the topic channel.
			topic <- job
			break
		}
		fmt.Println(callback, "of", job.X, "and", job.Y, "is", response.Result)
	}
}

// The subscribe function registers a worker to the topic, creating an RPC client,
// and will use the given callback string as the callback function whenever work
// is available.
func subscribe(topic string, factoryAddress string, callback string) (err error) {
	fmt.Println("Subscription request")
	fmt.Println("Factory address ---> ", factoryAddress)
	topicmx.RLock()
	ch := topics[topic]
	topicmx.RUnlock()
	client, err := rpc.Dial("tcp", factoryAddress)
	if err == nil {
		go subscriberLoop(ch, client, callback)
	} else {
		fmt.Println("Error subscribing ", factoryAddress)
		fmt.Println(err)
		return err
	}
	return
}

type Broker struct{}

func (b *Broker) CreateChannel(req stubs.ChannelRequest, res *stubs.StatusReport) (err error) {
	createTopic(req.Topic, req.Buffer)
	return
}

func (b *Broker) Subscribe(req stubs.Subscription, res *stubs.StatusReport) (err error) {
	err = subscribe(req.Topic, req.FactoryAddress, req.Callback)
	if err != nil {
		res.Message = "Error during subscription"
	}
	return err
}

func (b *Broker) Publish(req stubs.PublishRequest, res *stubs.StatusReport) (err error) {
	err = publish(req.Topic, req.Pair)
	return err
}

func main() {
	pAddr := flag.String("port", "8030", "Port to listen on")
	flag.Parse()
	rpc.Register(&Broker{})
	listener, _ := net.Listen("tcp", ":"+*pAddr)
	defer listener.Close()
	rpc.Accept(listener)
}
