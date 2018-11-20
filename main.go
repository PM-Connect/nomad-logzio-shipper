package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	consul "github.com/hashicorp/consul/api"
	nomad "github.com/hashicorp/nomad/api"
	"github.com/logzio/logzio-go"
	log "github.com/sirupsen/logrus"
)

type logItem map[string]string

type processStats struct {
	BytesRead int64     `json:"bytes"`
	LastSeen  time.Time `json:"last_seen"`
}

type metaConfig struct {
	Type                         string
	MapJobNameToProperty         string
	StdEndOfLogDelim             string
	NginxAccessLogsFileName      string
	NginxErrorLogsFileName       string
	ApplicationLogsFileName      string
	ApplicationLogsEndOfLogDelim string
	ApplicationLogsType          string
}

var initialized bool

// StdOut is the name of the log type that nomad uses to reference stdout.
const StdOut string = "stdout"

// StdErr is the name of the log type that nomad uses to reference stderr.
const StdErr string = "stderr"

func main() {
	var nomadAddr, nomadClientID, consulAddr, logzToken, logzAddr, consulPath string
	var verbose, randomisedRestart bool
	var maxAge int

	args := os.Args[1:]

	flags := flag.NewFlagSet("command", flag.ContinueOnError)
	flags.StringVar(&nomadAddr, "nomad", "http://127.0.0.1:4646", "The nomad address to talk to.")
	flags.StringVar(&nomadClientID, "node", "", "The ID of the nomad client/node to scrape logs from.")
	flags.StringVar(&consulAddr, "consul", "http://127.0.0.1:8500", "The consul address to talk to.")
	flags.StringVar(&logzToken, "logz-token", "", "Your logz.io token.")
	flags.StringVar(&logzAddr, "logz-addr", "https://listener-eu.logz.io:8071", "The logz.io endpoint.")
	flags.BoolVar(&verbose, "verbose", false, "Enable verbose logging.")
	flags.IntVar(&maxAge, "max-age", 7, "Set the maximum age in days for allocation log state to be stored in consul for.")
	flags.StringVar(&consulPath, "consul-path", "logzio-nomad", "The KV path in consul to store allocation log state.")
	flags.BoolVar(&randomisedRestart, "random-restart", false, "Should the streaming of logs be restarted at random intervals. (Mostly for testing.)")

	flags.Parse(args)

	args = flags.Args()

	if verbose {
		log.SetLevel(log.DebugLevel)
	}

	config := nomad.Config{
		Address: nomadAddr,
	}

	client, err := nomad.NewClient(&config)

	if err != nil {
		log.Fatal(err)
	}

	consulClient, err := consul.NewClient(consul.DefaultConfig())

	if err != nil {
		log.Fatal(err)
	}

	kv := consulClient.KV()

	l, err := logzio.New(
		logzToken,
		logzio.SetUrl(logzAddr),
		logzio.SetDrainDuration(time.Second*1),
		logzio.SetTempDirectory(".Queue"),
		logzio.SetDrainDiskThreshold(90),
	)

	if err != nil {
		log.Fatal(err)
	}

	allocationJobs := make(chan *nomad.Allocation)

	go syncAllocations(client, nomadClientID, allocationJobs)
	go allocationCleanup(client, kv, consulPath, maxAge)

	allocationWorkers := map[string]chan bool{}

	exit := make(chan string)

Loop:
	for {
		select {
		case reason := <-exit:
			log.Error(fmt.Sprintf("Exiting: %s", reason))
			break Loop
		case allocation := <-allocationJobs:
			allocationWorker, allocationWorkerAvailable := allocationWorkers[allocation.ID]

			if !allocationWorkerAvailable {
				allocationWorker = make(chan bool, 1)
				allocationWorkers[allocation.ID] = allocationWorker
			}

			if len(allocationWorker) == 0 {
				allocationWorker <- true

				log.Info(fmt.Sprintf("Starting for allocation: %s", allocation.ID))

				go func(allocation *nomad.Allocation) {
					defer func() {
						<-allocationWorker
					}()

					var wg sync.WaitGroup

					cancelChannels := []chan bool{}
					channels := map[string]chan bool{}

				GroupListLoop:
					for _, group := range allocation.Job.TaskGroups {
						if *group.Name == allocation.TaskGroup {
							for _, t := range group.Tasks {
								cancelStderr := make(chan bool)
								cancelStdout := make(chan bool)

								cancelChannels = append(cancelChannels, cancelStderr)
								cancelChannels = append(cancelChannels, cancelStdout)

								channels[t.Name+"_stderr"] = cancelStderr
								channels[t.Name+"_stdout"] = cancelStdout
							}

							break GroupListLoop
						}
					}

				GroupShipLoop:
					for _, group := range allocation.Job.TaskGroups {
						if *group.Name == allocation.TaskGroup {
							for _, task := range group.Tasks {
								wg.Add(2)

								stopStderr := make(chan struct{})
								stopStdout := make(chan struct{})

								conf := buildMetaConfig(allocation.Job.Meta)

								go shipLogs(StdErr, conf, &wg, allocation, task.Name, kv, client, consulPath, stopStdout, stopStderr, cancelChannels, channels[task.Name+"_stderr"], l)
								go shipLogs(StdOut, conf, &wg, allocation, task.Name, kv, client, consulPath, stopStderr, stopStdout, cancelChannels, channels[task.Name+"_stdout"], l)
							}

							break GroupShipLoop
						}
					}

					if randomisedRestart {
						go randomRestart(allocation.ID, cancelChannels)
					}

					log.Debug(fmt.Sprintf("Waiting on WaitGroup for alloc: %s", allocation.ID))

					wg.Wait()

					log.Warn(fmt.Sprintf("Finished collection for alloc: %s", allocation.ID))
				}(allocation)
			}
		}
	}
}

func getNomadAllocations(client *nomad.Client, nodeID string) ([]*nomad.Allocation, error) {
	query := nomad.QueryOptions{}

	allocations, _, err := client.Nodes().Allocations(nodeID, &query)

	return allocations, err
}

func syncAllocations(client *nomad.Client, nodeID string, allocationJobs chan<- *nomad.Allocation) {
	if initialized {
		nextTime := time.Now().Truncate(time.Second * 10)
		nextTime = nextTime.Add(time.Second * 10)
		time.Sleep(time.Until(nextTime))
	} else {
		initialized = true
	}

	allocations, err := getNomadAllocations(client, nodeID)

	if err != nil {
		log.Fatal(err)
	}

	for _, allocation := range allocations {
		allocationJobs <- allocation
	}

	syncAllocations(client, nodeID, allocationJobs)
}

func shipLogs(logType string, conf metaConfig, wg *sync.WaitGroup, allocation *nomad.Allocation, taskName string, kv *consul.KV, client *nomad.Client, consulPath string, listenChan chan struct{}, stopChan chan struct{}, cancelChannels []chan bool, cancel chan bool, l *logzio.LogzioSender) {
	defer wg.Done()

	log.Info(fmt.Sprintf("Shipping Logs: %s %s %s", allocation.ID, taskName, logType))

	bytePostionIdentifier := fmt.Sprintf("%s:%s:%s", allocation.ID, taskName, logType)

	pair, _, err := kv.Get(fmt.Sprintf("%s/%s", consulPath, bytePostionIdentifier), nil)

	if err != nil {
		log.Error(err)
		wg.Done()
		return
	}

	var offsetBytes int64

	if pair != nil {
		var stats processStats

		err := json.Unmarshal(pair.Value, &stats)

		if err != nil {
			log.Error(err)
			wg.Done()
			return
		}

		offsetBytes = stats.BytesRead
	} else {
		offsetBytes = int64(0)
	}

	var stream <-chan *nomad.StreamFrame
	var errors <-chan error
	var itemType, delim string

	switch logType {
	case "stderr":
		fallthrough
	case "stdout":
		stream, errors = client.AllocFS().Logs(allocation, true, taskName, logType, "start", offsetBytes, listenChan, nil)

		itemType = "nomad-" + logType

		if len(conf.Type) > 0 {
			itemType = conf.Type
		}

		delim = "\n"

		if len(conf.StdEndOfLogDelim) > 0 {
			delim = conf.StdEndOfLogDelim
		}
	case "nginx_access":
		stream, errors = client.AllocFS().Stream(allocation, conf.NginxAccessLogsFileName, "start", offsetBytes, listenChan, nil)
		itemType = "nginx"
		delim = "\n"
	case "nginx_error":
		stream, errors = client.AllocFS().Stream(allocation, conf.NginxErrorLogsFileName, "start", offsetBytes, listenChan, nil)
		itemType = "nginx-error"
		delim = "["
	case "application":
		stream, errors = client.AllocFS().Stream(allocation, conf.ApplicationLogsFileName, "start", offsetBytes, listenChan, nil)
		itemType = "nomad-application"

		if len(conf.ApplicationLogsType) > 0 {
			itemType = conf.ApplicationLogsType
		}
	default:
		log.Panic("Invalid log type provided.")
	}

	bytesRead := offsetBytes

StreamLoop:
	for {
		select {
		case err := <-errors:
			log.Error(err)

			for _, c := range cancelChannels {
				c <- true
			}
		case <-cancel:
			log.Warn(fmt.Sprintf("Received cancel for alloc: %s Task: %s Type: %s", allocation.ID, taskName, logType))
			listenChan <- struct{}{}
			break StreamLoop
		case data := <-stream:
			bytes := len(data.Data)

			if bytesRead > 0 && pair != nil {
				value := string(data.Data)

				log.Debug(fmt.Sprintf("%s %s %s %d", allocation.ID, taskName, logType, bytes))

				for _, line := range strings.Split(strings.TrimSuffix(value, delim), delim) {
					item := logItem{
						"message":    line,
						"type":       itemType,
						"allocation": allocation.ID,
						"task":       taskName,
						"job":        allocation.JobID,
						"group":      allocation.TaskGroup,
					}

					if len(conf.MapJobNameToProperty) > 0 {
						item[conf.MapJobNameToProperty] = allocation.JobID
					}

					msg, err := json.Marshal(item)

					if err != nil {
						log.Error(err)
						for _, c := range cancelChannels {
							c <- true
						}
						break
					}

					l.Send(msg)
				}
			}

			bytesRead = bytesRead + int64(bytes)

			stats := processStats{
				BytesRead: bytesRead,
				LastSeen:  time.Now(),
			}

			statsJSON, err := json.Marshal(stats)

			if err != nil {
				for _, c := range cancelChannels {
					c <- true
				}
				log.Error(err)
				break
			}

			p := &consul.KVPair{Key: fmt.Sprintf("%s/%s", consulPath, bytePostionIdentifier), Value: []byte(statsJSON)}

			_, err = kv.Put(p, nil)

			if err != nil {
				for _, c := range cancelChannels {
					c <- true
				}
				log.Error(err)
				break
			}

			pair = p
		}
	}

	log.Warn(fmt.Sprintf("Loop finished for alloc: %s", allocation.ID))
}

func allocationCleanup(nomadClient *nomad.Client, kv *consul.KV, consulPath string, maxAge int) {
	nextTime := time.Now().Truncate(time.Second * 60)
	nextTime = nextTime.Add(time.Second * 60)
	time.Sleep(time.Until(nextTime))

	pairs, _, err := kv.List(consulPath, nil)

	if err != nil {
		log.Error(err)
		allocationCleanup(nomadClient, kv, consulPath, maxAge)
	}

	now := time.Now()

	for _, pair := range pairs {
		var stats processStats

		err := json.Unmarshal(pair.Value, &stats)

		if err != nil {
			log.Error(err)
			break
		}

		age := now.Sub(stats.LastSeen).Hours() / 24

		if int(age) >= maxAge {
			log.Info(fmt.Sprintf("Deleting Key: %s", pair.Key))

			_, err := kv.Delete(pair.Key, nil)

			if err != nil {
				log.Error(err)
				break
			}
		}
	}

	allocationCleanup(nomadClient, kv, consulPath, maxAge)
}

func buildMetaConfig(meta map[string]string) metaConfig {
	config := metaConfig{}

	if value, ok := meta["logzio_type"]; ok {
		config.Type = value
	}

	if value, ok := meta["logzio_map_job_name_to_property"]; ok {
		config.MapJobNameToProperty = value
	}

	if value, ok := meta["logzio_srderr_end_of_log_delim"]; ok {
		config.StdEndOfLogDelim = value
	}

	if value, ok := meta["logzio_nginx_access_logs"]; ok {
		config.NginxAccessLogsFileName = value
	}

	if value, ok := meta["logzio_nginx_error_logs"]; ok {
		config.NginxErrorLogsFileName = value
	}

	if value, ok := meta["logzio_application_logs"]; ok {
		config.ApplicationLogsFileName = value
	}

	if value, ok := meta["logzio_application_end_of_log_delim"]; ok {
		config.ApplicationLogsEndOfLogDelim = value
	}

	if value, ok := meta["logzio_applocation_type"]; ok {
		config.ApplicationLogsType = value
	}

	return config
}

func randomRestart(allocationID string, cancelChannels []chan bool) {
	nextTime := time.Now().Truncate(time.Second * 30)
	nextTime = nextTime.Add(time.Second * 30)
	time.Sleep(time.Until(nextTime))

	chance := RandomWeightSelect(1, 100)

	if chance {
		log.Warn(fmt.Sprintf("Cancelling alloc: %s", allocationID))
		for _, c := range cancelChannels {
			c <- true
		}
	} else {
		randomRestart(allocationID, cancelChannels)
	}
}

// RandomWeightSelect returns true or false randomly, with weight.
func RandomWeightSelect(trueWeight int, falseWeight int) bool {
	rand.Seed(time.Now().UnixNano())

	r := rand.Intn(trueWeight + falseWeight)

	for _, b := range []bool{true, false} {
		if b == true {
			r -= trueWeight
		} else {
			r -= falseWeight
		}

		if r <= 0 {
			return b
		}
	}

	return false
}
