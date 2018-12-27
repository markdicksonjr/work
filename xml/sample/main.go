package main

import (
	"encoding/xml"
	"flag"
	"github.com/markdicksonjr/go-worker"
	xmlWorker "github.com/markdicksonjr/go-worker/xml"
	"log"
	"time"
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

// the function that's run by the worker (where the work happens)
func doWork(job worker.Job) error {
	log.Println("working")

	if job.Context != nil {
		record := (job.Context).(*xmlWorker.Record)
		log.Println("encountered " + record.TypeName);
		time.Sleep(time.Second * 3) // mimic a task that takes 3 seconds
	}

	return nil
}

// builds a function that converts an XML token to a struct we care about, firing DecodeEvents appropriately
func tokenRecordsBuilderFunction(reader xmlWorker.Reader) func(t xml.Token) xmlWorker.RecordsBuilderResult {
	return func(t xml.Token) xmlWorker.RecordsBuilderResult {

		// handle each token type of interest
		switch se := t.(type) {
		case xml.StartElement:
			if se.Name.Local == "addresses" {
				p := Addresses{}

				if err := reader.DecodeToken(&p, &se); err != nil {
					log.Fatal(err)
				}

				if len(p.Address) > 0 {

					// convert addresses to records/records builder result
					records := make([]*xmlWorker.Record, 0)
					for _, v := range p.Address {
						records = append(records, &xmlWorker.Record{
							TypeName: se.Name.Local,
							Data: v,
						})
					}
					return xmlWorker.RecordsBuilderResult{Records: records}
				}
			}

			if se.Name.Local == "address" {
				// NOTE: this should never be hit, as addresses will parse this portion implicitly
				log.Println(se.Name.Local)

				p := Address{}

				if err := reader.DecodeToken(&p, &se); err != nil {
					log.Fatal(err)
				}

				log.Println(p.Street)
			}
		}

		return xmlWorker.RecordsBuilderResult{}
	}
}

// do the decoding
func decode(dispatcher *worker.Dispatcher) {
	reader := xmlWorker.Reader{}
	err := reader.Open("test.xml")

	if err != nil {
		log.Fatal(err)
	}

	processFn := tokenRecordsBuilderFunction(reader)

	for {
		result := reader.BuildRecordsFromToken(processFn)

		if result.Err != nil {
			log.Fatal(err)
		}

		if result.Records != nil && len(result.Records) > 0 {
			for _, v := range result.Records {
				job := worker.Job{Name: "address processing", Context: v, IsEndOfStream: result.IsEndOfStream}
				dispatcher.EnqueueJob(job)
			}
		}

		if result.IsEndOfStream {
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

	// start the dispatcher
	dispatcher := worker.NewDispatcher(*maxQueueSize, *maxWorkers, doWork, worker.NoLogFunction) // fmt.Printf is also a good alternative
	dispatcher.Run()

	// start decoding
	decode(dispatcher)

	dispatcher.WaitUntilIdle()
}
