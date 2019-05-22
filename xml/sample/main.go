package main

import (
	"encoding/xml"
	"flag"
	"github.com/markdicksonjr/go-worker"
	xmlWorker "github.com/markdicksonjr/go-worker/xml"
	xmlWorkerBatch "github.com/markdicksonjr/go-worker/xml/batch"
	"log"
	"strconv"
	"time"
)

// app-specific struct for "Addresses" xml element
type Addresses struct {
	XMLName xml.Name  `xml:"addresses"`
	Address []Address `xml:"address"`
	Text    string    `xml:",chardata"`
}

// app-specific struct for "Address" xml element
type Address struct {
	XMLName xml.Name `xml:"address"`
	Text    string   `xml:",chardata"`
	Name    string   `xml:"name"`
	Street  string   `xml:"street"`
}

// the function that's run by the worker (where the work happens)
func doWork(job worker.Job, ctx *worker.Context) error {
	log.Println("working")

	if job.Context != nil {
		record := (job.Context).(xmlWorker.ProcessTokenResult)
		log.Println("encountered " + strconv.Itoa(len(record.Records)))
		time.Sleep(time.Second * 3) // mimic a task that takes 3 seconds
	}

	return nil
}

// builds a function that converts an XML token to a struct we want
func tokenRecordsBuilderFunction(reader *xmlWorkerBatch.Reader) func(t xml.Token) xmlWorker.RecordsBuilderResult {
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
							Data:     v,
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

func main() {
	var (
		maxQueueSize = flag.Int("max_queue_size", 100, "The size of job queue")
		maxBatchSize = flag.Int("max_batch_size", 100, "The max size of batches sent to workers")
		maxWorkers   = flag.Int("max_workers", 2, "The number of workers to start")
	)
	flag.Parse()

	// allocate the XML batch reader
	reader := xmlWorkerBatch.Reader{}
	reader.Init("address processing", *maxQueueSize, *maxWorkers, *maxBatchSize, doWork, worker.JobErrorsFatalLogFunction, worker.NoLogFunction) // fmt.Printf is also a good alternative

	// start decoding
	if err := reader.Decode("test.xml", tokenRecordsBuilderFunction(&reader)); err != nil {
		log.Fatal(err)
	}
}
