package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/alexeysofin/couchcopy/common"
	"github.com/alexeysofin/couchcopy/files"
	tm "github.com/buger/goterm"
	"github.com/exponent-io/jsonpath"
	"github.com/fatih/color"
	"github.com/vharitonsky/iniflags"
	"log"
	"os"
	"time"
)

var input = flag.String("input", "", "Couchdb input url with auth prefix, for ex. https://username:password@example.com/database/_all_docs?include_docs=true&reduce=false")
var output = flag.String("output", "", "Couchdb output url with auth prefix, for ex. https://username:password@example.com/database/")
var bulk = flag.Int("bulk", 5000, "Send bulk items in one http request")
var redshift = flag.Bool("redshift", false, "Convert a database to a redshift format (no commas, only documents, divided by new line)")

type Doc struct {
	Doc map[string]interface{}
}

func printOutput(input *string, output *string, total float64, processed float64) {
	// prints current progress using goterm library
	green := color.New(color.FgGreen).SprintFunc()

	current := processed / total * 100.0

	tm.Clear()
	tm.Printf("Source database: %s\n", green(*input))
	tm.Printf("Target database: %s\n", green(*output))
	tm.Printf("Total rows: %s\n", green(total))
	tm.Printf("Progress: %s of %s (%s)", green(int(processed)), green(int(total)), green(fmt.Sprintf("%d%%", int(current))))
	tm.MoveCursor(1, 1)
	tm.Flush()
}

func updateOutput(input *string, output *string, total float64, rowDone <-chan int, finished <-chan bool) {
	// calls printOutput every second,
	// or if processing is finished (we get a value from finished channel)
	// once we get a value from rowDone, we update a counter (one more row has been processed)

	processed := 0.0

	ticker := time.NewTicker(time.Millisecond * 1000)

	for {
		select {
		case <-rowDone:
			processed++
		case <-ticker.C:
			printOutput(input, output, total, processed)
		case <-finished:
			printOutput(input, output, total, total)
		default:
		}
	}
}

func pushDocsWorker(url *string, docs chan *Doc, done chan bool, redshift bool) {
	if redshift {
		// if we are converting to redshift format (writing to local file)
		// we do not need any buffered docs, just write them do output file with a new line
		fp, err := os.Create(*url)
		common.CheckError(err)
		writer := bufio.NewWriter(fp)

		defer fp.Close()

		for doc := range docs {
			encoded, _ := json.Marshal(doc.Doc)
			files.WriteLineToFile(writer, encoded)
		}

		writer.Flush()

	} else {
		// buffers interface{} (Doc.Doc) objects into array (for bulk sending) from docs channel
		// if buffer is full, we push docs to remote database
		buffer := make([]interface{}, *bulk)
		counter := 0

		for doc := range docs {
			buffer[counter] = doc.Doc
			counter++
			if counter == *bulk {
				files.WriteToRemote(*url, buffer)
				counter = 0
			}
		}

		// push last chunk
		if counter > 0 && len(buffer) > 0 {
			files.WriteToRemote(*url, buffer[:counter])
		}

	}

	// send a value to one channel, because main goroutine can finished earlier
	// and should wait a value from this channel in the end
	done <- true
}

func main() {

	iniflags.Parse()

	if *input == "" || *output == "" {
		flag.PrintDefaults()
		os.Exit(2)
	}

	// connect to an input database
	reader, err := files.NewReader(*input)
	common.CheckError(err)

	// if output url is not a url and we are not converting to redshift,
	// we must save overall input as is
	// otherwise we are uploading to a database or converting to redshift
	if !files.IsPathUrl(*output) && !(*redshift) {
		written, err := files.WriteStreamToFile(*output, reader)
		common.CheckError(err)
		log.Printf("%d bytes written", written)
		os.Exit(0)
	}

	if files.IsPathUrl(*output) && (*redshift) {
		log.Fatal("Cannot convert to redshift with remote output")
	}

	decoder := jsonpath.NewDecoder(reader)

	var totalRows float64

	// decode a total count
	decoder.SeekTo("total_rows")
	decoder.Decode(&totalRows)

	// seek to rows key
	decoder.SeekTo("rows")

	// read open bracket (we want what's inside array)
	if _, err := decoder.Token(); err != nil {
		common.CheckError(err)
	}

	// channel for telling updateOutput goroutine about a new processed row
	processedChan := make(chan int)
	// channel for telling updateOutput goroutine that we are done processing
	// for it to print last output (to make 100% always appear on screen)
	finishedChan := make(chan bool)
	if *output != "" {
		go updateOutput(input, output, totalRows, processedChan, finishedChan)
	}

	// buffer channel for processing rows
	docsChan := make(chan *Doc, *bulk)
	// channel that we wait a value from in the end (to wait until pushDocsWorker finishes)
	done := make(chan bool)
	if *output != "" {
		go pushDocsWorker(output, docsChan, done, *redshift)
	}

	// while array contains rows
	for decoder.More() {

		var doc Doc

		if err := decoder.Decode(&doc); err != nil {
			common.CheckError(err)
		}

		//item, _ := json.Marshal(doc)

		//remove _rev field to exclude 412 conflict errors
		delete(doc.Doc, "_rev")

		docsChan <- &doc

		processedChan <- 1

	}

	// read closing bracket to close array
	if _, err := decoder.Token(); err != nil {
		common.CheckError(err)
	}
	// close channel for range to finish in pushDocsWorker
	close(docsChan)
	// print last output message
	finishedChan <- true
	// wait until pushDocsWorker finishes
	<-done

	os.Exit(0)
}
