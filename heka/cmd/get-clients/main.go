/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

/*

A command-line utility for getting data by clientId.

*/
package main

import (
	"bufio"
	"code.google.com/p/gogoprotobuf/proto"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/mozilla-services/heka/message"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

type MessageLocation struct {
	Key    string
	Offset uint32
	Length uint32
}

type OffsetCache struct {
	messages map[string][]MessageLocation
	count    int
}

func NewOffsetCache() *OffsetCache {
	return &OffsetCache{map[string][]MessageLocation{}, 0}
}

func (o *OffsetCache) Add(clientId string, offset MessageLocation) {
	entries, ok := o.messages[clientId]
	if !ok {
		entries = []MessageLocation{}
	}

	o.messages[clientId] = append(entries, offset)
	o.count++
}

func (o *OffsetCache) Get(clientId string) []MessageLocation {
	entries, ok := o.messages[clientId]
	if !ok {
		return []MessageLocation{}
	}
	return entries
}

func (o *OffsetCache) Size() int {
	return o.count
}

func main() {
	flagMatch := flag.String("match", "TRUE", "message_matcher filter expression")
	flagFormat := flag.String("format", "txt", "output format [txt|json|heka|count]")
	flagOutput := flag.String("output", "", "output filename, defaults to stdout")
	flagOffsets := flag.String("offsets", "", "file containing offset info")
	flagBucket := flag.String("bucket", "", "S3 Bucket name")
	flagAWSKey := flag.String("aws-key", "", "AWS Key")
	flagAWSSecretKey := flag.String("aws-secret-key", "", "AWS Secret Key")
	flagAWSRegion := flag.String("aws-region", "us-west-2", "AWS Region")
	flagMaxMessageSize := flag.Uint64("max-message-size", 4*1024*1024, "maximum message size in bytes")
	flagWorkers := flag.Uint64("workers", 16, "number of parallel workers")
	flag.Parse()

	if *flagMaxMessageSize < math.MaxUint32 {
		maxSize := uint32(*flagMaxMessageSize)
		message.SetMaxMessageSize(maxSize)
	} else {
		fmt.Printf("Message size is too large: %d\n", flagMaxMessageSize)
		os.Exit(8)
	}

	workers := 1
	if *flagWorkers < math.MaxUint32 {
		workers = int(*flagWorkers)
	} else if *flagWorkers == 0 {
		fmt.Printf("Cannot run with zero workers. Using 1.\n")
	} else {
		fmt.Printf("Too many workers: %d. Are you crazy?\n", flagWorkers)
		os.Exit(8)
	}

	var err error
	var match *message.MatcherSpecification
	if match, err = message.CreateMatcherSpecification(*flagMatch); err != nil {
		fmt.Printf("Match specification - %s\n", err)
		os.Exit(2)
	}

	var out *os.File
	if "" == *flagOutput {
		out = os.Stdout
	} else {
		if out, err = os.OpenFile(*flagOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
			fmt.Printf("%s\n", err)
			os.Exit(3)
		}
		defer out.Close()
	}

	offsets, err := readOffsets(*flagOffsets)
	if err != nil {
		fmt.Printf("Error reading offsets file: %s\n", err)
		os.Exit(9)
	}

	fmt.Printf("Loaded %d offsets\n", offsets.Size())

	auth, err := aws.GetAuth(*flagAWSKey, *flagAWSSecretKey, "", time.Now())
	if err != nil {
		fmt.Printf("Authentication error: %s\n", err)
		os.Exit(4)
	}
	region, ok := aws.Regions[*flagAWSRegion]
	if !ok {
		fmt.Printf("Parameter 'aws-region' must be a valid AWS Region\n")
		os.Exit(5)
	}
	s := s3.New(auth, region)
	bucket := s.Bucket(*flagBucket)

	clientIdChannel := make(chan string, 1000)
	recordChannel := make(chan []byte, 1000)
	done := make(chan bool)
	clientsDone := make(chan string, 1000)
	doneSaving := make(chan bool)

	for i := 1; i <= workers; i++ {
		go getClientRecords(bucket, offsets, clientIdChannel, recordChannel, done, clientsDone)
	}
	go saveRecords(recordChannel, doneSaving, *flagFormat, match, out)

	startTime := time.Now().UTC()
	totalClientIds := 0
	pendingClientIds := 0
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		clientId := scanner.Text()
		totalClientIds++
		pendingClientIds++
		clientIdChannel <- clientId
		if pendingClientIds == 1000 {
			// Wait for one to finish so we don't fill up our channels:
			waitForClients(clientsDone, 1)
			pendingClientIds--
		}
	}

	close(clientIdChannel)
	<-done

	fmt.Printf("Waiting for %d more clients\n", pendingClientIds)
	waitForClients(clientsDone, pendingClientIds)
	close(recordChannel)

	<-doneSaving
	duration := time.Now().UTC().Sub(startTime).Seconds()
	fmt.Printf("All done processing %d clientIds in %.2f seconds\n", totalClientIds, duration)
}

func waitForClients(clientsDone <-chan string, count int) {
	// Wait for one or more clientIds to complete
	for i := 1; i <= count; i++ {
		<-clientsDone
	}
}

func makeInt(numstr string) (uint32, error) {
	i, err := strconv.ParseInt(string(numstr), 10, 64)
	if err != nil {
		return 0, err
	}
	if i < 0 || i > math.MaxUint32 {
		return 0, fmt.Errorf("Error parsing %d as uint32")
	}
	return uint32(i), nil
}

func getClientRecords(bucket *s3.Bucket, offsets *OffsetCache, todoChannel <-chan string, recordChannel chan<- []byte, done chan<- bool, clientsDone chan<- string) {
	ok := true
	for ok {
		clientId, ok := <-todoChannel
		if !ok {
			// Channel is closed
			done <- true
			break
		}

		var headers = map[string][]string{
			"Range": []string{""},
		}

		for _, o := range offsets.Get(clientId) {

			headers["Range"][0] = fmt.Sprintf("bytes=%d-%d", o.Offset, o.Offset+o.Length-1)
			record, err := getClientRecord(bucket, &o, headers)
			if err != nil {
				fmt.Printf("Error fetching %s @ %d+%d: %s\n", o.Key, o.Offset, o.Length, err)
				continue
			}
			recordChannel <- record
		}
		clientsDone <- clientId
	}
}

func getClientRecord(bucket *s3.Bucket, o *MessageLocation, headers map[string][]string) ([]byte, error) {
	resp, err := bucket.GetResponseWithHeaders(o.Key, headers)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if len(body) != int(o.Length) {
		fmt.Printf("Unexpected body length: %d != %d\n", len(body), o.Length)
	}
	return body, err
}

func saveRecords(recordChannel <-chan []byte, done chan<- bool, format string, match *message.MatcherSpecification, out *os.File) {
	processed := 0
	matched := 0
	bytes := 0
	msg := new(message.Message)
	header := new(message.Header)
	ok := true
	recordSep := []byte{message.RECORD_SEPARATOR}
	unitSep := []byte{message.UNIT_SEPARATOR}

	for ok {
		record, ok := <-recordChannel
		if !ok {
			// Channel is closed
			done <- true
			break
		}

		bytes += len(record)

		processed += 1
		if err := proto.Unmarshal(record, msg); err != nil {
			fmt.Printf("Error unmarshalling message: %s\n", err)
			continue
		}

		if !match.Match(msg) {
			continue
		}

		matched += 1

		switch format {
		case "count":
			// no op
		case "json":
			contents, _ := json.Marshal(msg)
			fmt.Fprintf(out, "%s\n", contents)
		case "heka":
			// Frame the record with a header + separators
			header.SetMessageLength(uint32(len(record)))
			headerBytes, err := header.Marshal()
			if err != nil {
				fmt.Printf("Error marshaling record header: %s\n", err)
				continue
			}
			out.Write(recordSep)
			out.Write([]byte{byte(len(headerBytes))})
			out.Write(headerBytes)
			out.Write(unitSep)
			out.Write(record)
		default:
			fmt.Fprintf(out, "Timestamp: %s\n"+
				"Type: %s\n"+
				"Hostname: %s\n"+
				"Pid: %d\n"+
				"UUID: %s\n"+
				"Logger: %s\n"+
				"Payload: %s\n"+
				"EnvVersion: %s\n"+
				"Severity: %d\n"+
				"Fields: %+v\n\n",
				time.Unix(0, msg.GetTimestamp()), msg.GetType(),
				msg.GetHostname(), msg.GetPid(), msg.GetUuidString(),
				msg.GetLogger(), msg.GetPayload(), msg.GetEnvVersion(),
				msg.GetSeverity(), msg.Fields)
		}
	}
	fmt.Printf("Processed: %d, matched: %d messages (%.2f MB)\n", processed, matched, (float64(bytes) / 1024.0 / 1024.0))
}

func readOffsets(filename string) (*OffsetCache, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// clientId -> locations
	var offsets *OffsetCache
	offsets = NewOffsetCache()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		pieces := strings.Split(scanner.Text(), "\t")
		lineNum++
		if len(pieces) != 4 {
			return nil, fmt.Errorf("Error on line %d: invalid line. Expected 4 values, found %d.", lineNum, len(pieces))
		}
		o, err := makeInt(pieces[2])
		if err != nil {
			return nil, err
		}
		l, err := makeInt(pieces[3])
		if err != nil {
			return nil, err
		}
		offsets.Add(pieces[1], MessageLocation{pieces[0], o, l})
	}
	fmt.Printf("Successfully processed %d offset lines\n", lineNum)
	return offsets, nil
}
