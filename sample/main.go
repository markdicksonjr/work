package main

import (
	"encoding/xml"
	"flag"
	"log"
	"time"
	"github.com/markdicksonjr/go-workers"
	xmlWorker "github.com/markdicksonjr/go-workers/xml"
)

// app-specific struct for "Addresses" xml element
type Addresses struct {
	XMLName xml.Name	`xml:"addresses"`
	Address []Address	`xml:"address"`
	Text	string		`xml:",chardata"`
}

// app-specific struct for "Address" xml element
type Address struct {
	XMLName	xml.Name	`xml:"address"`
	Text	string		`xml:",chardata"`
	Name	string		`xml:"name"`
	Street	string		`xml:"street"`
}

// generic interface for when a structure we care about has been encountered by the reader
type DecodeEvents interface {
	onAddress(address Address)
	onError(err error) (fatal bool)
}

// implementation of DecodeEvents, which holds dispatcher and job queue
type AddressDecodeEvents struct {
	dispatcher *worker.Dispatcher
	jobQueue chan worker.Job
}

// address handler for xml reader processing (queues job)
func (a AddressDecodeEvents) onAddress(address Address) {
	// TODO: batch these up and send them off to DB?
	log.Println("Got Address on event handler")

	job := worker.Job{Name: "address processing", Context: &address}
	a.jobQueue <- job
}

// error handler from xml reader processing
func (a AddressDecodeEvents) onError(err error) bool {
	log.Println("Error: ", err)
	return true
}

// the function that's run by the worker (where the work happens)
func doWork(job worker.Job) error {
	time.Sleep(time.Second * 3) // TODO: mimic a job that takes 3 seconds
	return nil
}

// builds a function that converts an XML token to a struct we care about, firing DecodeEvents appropriately
func generateTokenProcessor(reader xmlWorker.Reader, events DecodeEvents) func(t xml.Token) error {
	return func(t xml.Token) error {
		// TODO: reflection could be slow - perhaps have workers to reflect & decode as well as save?
		// handle each token type of interest
		switch se := t.(type) {
		case xml.StartElement:
			if se.Name.Local == "addresses" {
				p := Addresses{}
				reader.DecodeToken(&p, &se)

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
				reader.DecodeToken(&p, &se)

				log.Println(p.Street)
			}
		}

		return nil
	}
}

// do the decoding
func decode(events DecodeEvents) {
	reader := xmlWorker.Reader{}
	err := reader.Open("test.xml")

	if err != nil {
		log.Fatal(err)
	}

	isEof := false

	processFn := generateTokenProcessor(reader, events)

	for {
		isEof, err = reader.ProcessToken(processFn)

		if err != nil {
			log.Fatal(err)
		}

		if isEof {
			break
		}
	}
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
	dispatcher := worker.NewDispatcher(jobQueue, *maxWorkers, doWork, worker.NoLogFunction) // fmt.Printf is also a good alternative
	dispatcher.Run()

	// start decoding
	events := AddressDecodeEvents{
		dispatcher: dispatcher,
		jobQueue: jobQueue,
	}
	decode(&events)

	dispatcher.WaitUntilIdle()
}
