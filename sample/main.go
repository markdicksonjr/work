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

// the function that's run by the worker (where the work happens)
func doWork(job worker.Job) error {
	time.Sleep(time.Second * 3) // TODO: mimic a job that takes 3 seconds
	return nil
}

// builds a function that converts an XML token to a struct we care about, firing DecodeEvents appropriately
func generateTokenProcessor(reader xmlWorker.Reader) func(t xml.Token) xmlWorker.RecordsBuilderResult {
	return func(t xml.Token) xmlWorker.RecordsBuilderResult {
		// TODO: reflection could be slow - perhaps have workers to reflect & decode as well as save?
		// handle each token type of interest
		switch se := t.(type) {
		case xml.StartElement:
			if se.Name.Local == "addresses" {
				p := Addresses{}
				reader.DecodeToken(&p, &se)

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
				reader.DecodeToken(&p, &se)

				log.Println(p.Street)
			}
		}

		return xmlWorker.RecordsBuilderResult{}
	}
}

// do the decoding
func decode(jobQueue chan worker.Job) {
	reader := xmlWorker.Reader{}
	err := reader.Open("test.xml")

	if err != nil {
		log.Fatal(err)
	}

	processFn := generateTokenProcessor(reader)

	for {
		result := reader.BuildRecordsFromToken(processFn)

		if result.Err != nil {
			log.Fatal(err)
		}

		if result.Records != nil && len(result.Records) > 0 {
			for _, v := range result.Records {
				job := worker.Job{Name: "address processing", Context: &v, IsEndOfStream: result.IsEndOfStream}
				jobQueue <- job
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

	// create the job queue
	jobQueue := make(chan worker.Job, *maxQueueSize)

	// start the dispatcher
	dispatcher := worker.NewDispatcher(jobQueue, *maxWorkers, doWork, worker.NoLogFunction) // fmt.Printf is also a good alternative
	dispatcher.Run()

	// start decoding
	decode(jobQueue)

	dispatcher.WaitUntilIdle()
}
