package main

import (
	"encoding/xml"
	"flag"
	"io"
	"log"
	"os"
	"time"
	"github.com/markdicksonjr/go-workers"
	"fmt"
)

type Addresses struct {
	XMLName xml.Name	`xml:"addresses"`
	Address []Address	`xml:"address"`
	Text	string		`xml:",chardata"`
}

type Address struct {
	XMLName	xml.Name	`xml:"address"`
	Text	string		`xml:",chardata"`
	Name	string		`xml:"name"`
	Street	string		`xml:"street"`
}

type DecodeEvents interface {
	onAddress(address Address)
	onError(err error) (fatal bool)
}

func decode(events DecodeEvents) {
	xmlFile, _ := os.Open("test.xml")
	decoder := xml.NewDecoder(xmlFile)

	for {

		// decode a token
		t, err := decoder.Token()

		// return an error, if one happened
		if err != nil {
			if err == io.EOF {
				break
			}

			if events.onError(err) {
				break
			}
		}

		// stop looping when we have no more tokens
		if t == nil {
			break
		}

		// TODO: reflection could be slow - perhaps have workers to reflect & decode as well as save?
		// handle each token type of interest
		switch se := t.(type) {
		case xml.StartElement:
			if se.Name.Local == "addresses" {
				p := Addresses{}
				decoder.DecodeElement(&p, &se)

				if len(p.Address) > 0 {
					for _, v := range p.Address {
						events.onAddress(v)
					}
				}
			}

			if se.Name.Local == "address" {
				// NOTE: this should never be hit, as addresses will parse this portion implicitly
				log.Println(se.Name.Local)

				p := Address{}
				decoder.DecodeElement(&p, &se)

				log.Println(p.Street)
			}
		}
	}
}

type AddressDecodeEvents struct {
	dispatcher *worker.Dispatcher
	jobQueue chan worker.Job
}

func (a AddressDecodeEvents) onAddress(address Address) {
	// TODO: batch these up and send them off to DB?
	log.Println("Got Address on event handler")

	job := worker.Job{Name: "address processing", Context: &address}
	a.jobQueue <- job
}

func (a AddressDecodeEvents) onError(err error) bool {
	log.Println("Error: ", err)
	return true
}

func doWork(job worker.Job) error {
	time.Sleep(time.Second * 3) // TODO: mimic a job that takes 3 seconds
	return nil
}

func main() {
	var (
		maxQueueSize = flag.Int("max_queue_size", 100, "The size of job queue")
		maxWorkers   = flag.Int("max_workers", 2, "The number of workers to start")
	)
	flag.Parse()

	// create the job queue
	jobQueue := make(chan worker.Job, *maxQueueSize)

	// start the dispatcher
	dispatcher := worker.NewDispatcher(jobQueue, *maxWorkers, doWork, fmt.Printf)
	dispatcher.Run()

	// start decoding
	events := AddressDecodeEvents{
		dispatcher: dispatcher,
		jobQueue: jobQueue,
	}
	decode(&events)

	dispatcher.WaitUntilIdle()
}