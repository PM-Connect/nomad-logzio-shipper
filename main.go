package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strconv"
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
	BytesRead   int64     `json:"bytes"`
	OffsetBytes int64     `json:"offset"`
	LastSeen    time.Time `json:"last_seen"`
}

type metaConfig struct {
	MapJobNameToProperty string
	LogFiles             []logFileConfig
}

type taskMetaConfig struct {
	Type  string
	Delim string
}

type logFileConfig struct {
	Path  string
	Delim string
	Type  string
}

var initialized bool

// StdOut is the name of the log type that nomad uses to reference stdout.
const StdOut string = "stdout"

// StdErr is the name of the log type that nomad uses to reference stderr.
const StdErr string = "stderr"

func main() {
	var nomadAddr, nomadClientID, consulAddr, logzToken, logzAddr, consulPath, queueDir string
	var verbose, randomisedRestart bool
	var maxAge int

	args := os.Args[1:]

	flags := flag.NewFlagSet("command", flag.ContinueOnError)
	flags.StringVar(&nomadAddr, "nomad", "http://127.0.0.1:4646", "The nomad address to talk to.")
	flags.StringVar(&nomadClientID, "node", "", "The ID of the nomad client/node to scrape logs from.")
	flags.StringVar(&consulAddr, "consul", "127.0.0.1:8500", "The consul address to talk to.")
	flags.StringVar(&logzToken, "logz-token", "", "Your logz.io token.")
	flags.StringVar(&logzAddr, "logz-addr", "https://listener-eu.logz.io:8071", "The logz.io endpoint.")
	flags.BoolVar(&verbose, "verbose", false, "Enable verbose logging.")
	flags.IntVar(&maxAge, "max-age", 7, "Set the maximum age in days for allocation log state to be stored in consul for.")
	flags.StringVar(&consulPath, "consul-path", "logzio-nomad", "The KV path in consul to store allocation log state.")
	flags.BoolVar(&randomisedRestart, "random-restart", false, "Should the streaming of logs be restarted at random intervals. (Mostly for testing.)")
	flags.StringVar(&queueDir, "queue-dir", ".Queue", "The directory to store logzio messages before sending.")

	flags.Parse(args)

	args = flags.Args()

	if envToken := os.Getenv("LOGZIO_TOKEN"); logzToken == "" && envToken != "" {
		logzToken = envToken
	} else if logzToken == "" {
		log.Fatal("No logzio token provided as an argument or in the LOGZIO_TOKEN env variable.")
	}

	if envNomadClientID := os.Getenv("NOMAD_CLIENT_ID"); nomadClientID == "" && envNomadClientID != "" {
		nomadClientID = envNomadClientID
	} else if nomadClientID == "" {
		log.Fatal("No nomad client/node id provided as an argument or in the NOMAD_CLIENT_ID env variable.")
	}

	if verbose {
		log.SetLevel(log.DebugLevel)
	}

	config := nomad.Config{
		Address: nomadAddr,
	}

	client, err := nomad.NewClient(&config)

	if err != nil {
		log.Panic(err)
	}

	consulConfig := consul.DefaultConfig()

	consulConfig.Address = consulAddr

	consulClient, err := consul.NewClient(consulConfig)

	if err != nil {
		log.Panic(err)
	}

	kv := consulClient.KV()

	l, err := logzio.New(
		logzToken,
		logzio.SetUrl(logzAddr),
		logzio.SetDrainDuration(time.Second*1),
		logzio.SetTempDirectory(queueDir),
		logzio.SetDrainDiskThreshold(90),
	)

	if err != nil {
		log.Panic(err)
	}

	allocationJobs := make(chan *nomad.Allocation)
	allocationWorkers := map[string]chan bool{}

	go syncAllocations(client, nomadClientID, allocationJobs)
	go allocationCleanup(client, kv, consulPath, maxAge)

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

					var allocGroup *nomad.TaskGroup

				GroupListLoop:
					for _, group := range allocation.Job.TaskGroups {
						if *group.Name == allocation.TaskGroup {
							allocGroup = group

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

					conf := buildMetaConfig(allocGroup.Meta)

					for i := range conf.LogFiles {
						c := make(chan bool)
						cancelChannels = append(cancelChannels, c)
						channels["file_"+strconv.Itoa(i)] = c
					}

				GroupShipLoop:
					for _, group := range allocation.Job.TaskGroups {
						if *group.Name == allocation.TaskGroup {
							for _, task := range group.Tasks {
								wg.Add(2)

								stopStderr := make(chan struct{})
								stopStdout := make(chan struct{})

								taskConfig := buildTaskMetaConfig(task.Meta)

								go shipLogs(StdErr, conf, &taskConfig, &wg, allocation, task.Name, kv, client, consulPath, stopStderr, filterCancelChannels(cancelChannels, channels[task.Name+"_stderr"]), channels[task.Name+"_stderr"], l, nil)
								go shipLogs(StdOut, conf, &taskConfig, &wg, allocation, task.Name, kv, client, consulPath, stopStdout, filterCancelChannels(cancelChannels, channels[task.Name+"_stdout"]), channels[task.Name+"_stdout"], l, nil)
							}

							break GroupShipLoop
						}
					}

					for i := range conf.LogFiles {
						wg.Add(1)
						stop := make(chan struct{})
						c := channels["file_"+strconv.Itoa(i)]
						go shipLogs("file", conf, nil, &wg, allocation, "leader", kv, client, consulPath, stop, filterCancelChannels(cancelChannels, c), c, l, &conf.LogFiles[i])
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

		time.Sleep(time.Millisecond * 100)
	}
}

func getNomadAllocations(client *nomad.Client, nodeID string) ([]*nomad.Allocation, error) {
	query := nomad.QueryOptions{}

	allocations, _, err := client.Nodes().Allocations(nodeID, &query)

	return allocations, err
}

func syncAllocations(client *nomad.Client, nodeID string, allocationJobs chan<- *nomad.Allocation) {
	if initialized {
		nextTime := time.Now().Truncate(time.Second * 20)
		nextTime = nextTime.Add(time.Second * 20)
		time.Sleep(time.Until(nextTime))
	} else {
		initialized = true
	}

	allocations, err := getNomadAllocations(client, nodeID)

	if err != nil {
		log.Panic(err)
	}

	for _, allocation := range allocations {
		if allocation.ClientStatus == "running" {
			allocationJobs <- allocation
		}
	}

	syncAllocations(client, nodeID, allocationJobs)
}

func shipLogs(logType string, conf metaConfig, taskConf *taskMetaConfig, wg *sync.WaitGroup, allocation *nomad.Allocation, taskName string, kv *consul.KV, client *nomad.Client, consulPath string, stopChan chan struct{}, cancelChannels []chan bool, cancel chan bool, l *logzio.LogzioSender, logFile *logFileConfig) {
	defer wg.Done()

	allocation, _, err := client.Allocations().Info(allocation.ID, nil)

	if err != nil {
		log.Error("Error fetching allocation: ", err)
		for _, c := range cancelChannels {
			c <- true
		}
		return
	}

	if allocation.ClientStatus != "running" {
		log.Error("Error fetching allocation: ", err)
		for _, c := range cancelChannels {
			c <- true
		}
		return
	}

	if logFile != nil {
		log.Info(fmt.Sprintf("Shipping Logs: %s %s %s %s", allocation.ID, taskName, logType, logFile.Path))
	} else {
		log.Info(fmt.Sprintf("Shipping Logs: %s %s %s", allocation.ID, taskName, logType))
	}

	var bytePostionIdentifier string

	if logFile == nil {
		bytePostionIdentifier = fmt.Sprintf("%s:%s:%s", allocation.ID, taskName, logType)
	} else {
		bytePostionIdentifier = fmt.Sprintf("%s:%s:%s:%s", allocation.ID, taskName, logType, strings.Replace(logFile.Path, "/", "-", -1))
	}

	pair, _, err := kv.Get(fmt.Sprintf("%s/%s", consulPath, bytePostionIdentifier), nil)

	if err != nil {
		log.Error("Error fetching consul log shipping stats: ", err)
		for _, c := range cancelChannels {
			c <- true
		}
		return
	}

	var offsetBytes int64
	var bytesRead int64
	var stats processStats

	if pair != nil {
		err := json.Unmarshal(pair.Value, &stats)

		if err != nil {
			log.Error("Error converting consul data to struct: ", err)
			for _, c := range cancelChannels {
				c <- true
			}
			return
		}

		offsetBytes = stats.OffsetBytes
		bytesRead = stats.BytesRead
	} else {
		offsetBytes = int64(0)
		bytesRead = int64(0)
	}

	var stream <-chan *nomad.StreamFrame
	var errors <-chan error
	var itemType, delim string

	switch logType {
	case "stderr":
		fallthrough
	case "stdout":
		data, _ := client.AllocFS().Logs(allocation, false, taskName, logType, "start", offsetBytes, stopChan, nil)

		content := <-data

		if content != nil {
			size := len(content.Data)

			if int64(size) < offsetBytes {
				offsetBytes = 0
			}
		}

		stream, errors = client.AllocFS().Logs(allocation, true, taskName, logType, "start", offsetBytes, stopChan, nil)

		itemType = "nomad-" + logType

		if taskConf != nil && len(taskConf.Type) > 0 {
			itemType = taskConf.Type
		}

		delim = "\n"

		if taskConf != nil && len(taskConf.Delim) > 0 {
			delim = taskConf.Delim
		}
	case "file":
		if logFile == nil {
			log.Error("Attempted to log file with nill logFileConfig.")
			for _, c := range cancelChannels {
				c <- true
			}
			return
		}

		data, _, err := client.AllocFS().Stat(allocation, logFile.Path, nil)

	FileExistanceLoop:
		for err != nil {
			if strings.Contains(err.Error(), "no such file or directory") {
				log.Warning("Unable to find file, retrying in 5s: ", err)
				allocation, _, allocErr := client.Allocations().Info(allocation.ID, nil)

				if allocErr != nil {
					log.Error("Unable to find alloc: ", allocErr)
					break FileExistanceLoop
				}

				if allocation.ClientStatus != "running" {
					log.Warning(fmt.Sprintf("Allocation is stopped: %s", allocation.ID))
					break FileExistanceLoop
				}

				time.Sleep(time.Second * 5)
				select {
				case <-cancel:
					log.Warn(fmt.Sprintf("Received cancel for alloc: %s Task: %s Type: %s", allocation.ID, taskName, logType))
					break FileExistanceLoop
				default:
					data, _, err = client.AllocFS().Stat(allocation, logFile.Path, nil)
				}
			} else {
				break FileExistanceLoop
			}
		}

		if err != nil {
			log.Error("Error calculating file size: ", err)
			for _, c := range cancelChannels {
				c <- true
			}
			return
		}

		if data.Size < offsetBytes {
			offsetBytes = 0
		}

		stream, errors = client.AllocFS().Stream(allocation, logFile.Path, "start", offsetBytes, stopChan, nil)

		if len(logFile.Type) == 0 {
			log.Error("Log file type must be set.")
			for _, c := range cancelChannels {
				c <- true
			}
			return
		}

		itemType = logFile.Type

		if len(logFile.Delim) > 0 {
			delim = logFile.Delim
		}
	default:
		log.Panic("Invalid log type provided.")
	}

StreamLoop:
	for {
		select {
		case err := <-errors:
			if strings.Contains(err.Error(), "no such file or directory") {
				log.Warning("Unable to find file: ", err)
			} else {
				log.Error("Error while streaming: ", err)
			}

			for _, c := range cancelChannels {
				c <- true
			}

			break StreamLoop
		case <-cancel:
			log.Warn(fmt.Sprintf("Received cancel for alloc: %s Task: %s Type: %s", allocation.ID, taskName, logType))
			stopChan <- struct{}{}
			break StreamLoop
		case data := <-stream:
			var bytes int

			if len(data.FileEvent) > 0 {
				offsetBytes = 0
			} else {
				bytes = len(data.Data)

				if offsetBytes > 0 && pair != nil {
					value := string(data.Data)

					log.Debug(fmt.Sprintf("%s %s %s %d", allocation.ID, taskName, logType, bytes))

					logItems := []logItem{
						logItem{
							"message": "",
						},
					}

					reg, err := regexp.Compile(delim)

					if err != nil {
						log.Error("Error compiling regex: ", err)
						for _, c := range cancelChannels {
							c <- true
						}
						break
					}

					for _, line := range strings.Split(value, "\n") {
						if len(line) == 0 {
							continue
						}

						if reg.MatchString(line) || delim == "\n" {
							if _, ok := logItems[len(logItems)-1]["message"]; ok && len(logItems[len(logItems)-1]["message"]) > 0 {
								logItems = append(logItems, logItem{
									"message": line,
								})
							} else {
								if len(logItems[len(logItems)-1]["message"]) > 0 {
									logItems[len(logItems)-1]["message"] = logItems[len(logItems)-1]["message"] + "\n" + line
								} else {
									logItems[len(logItems)-1]["message"] = line
								}
							}
						} else {
							if len(logItems[len(logItems)-1]["message"]) > 0 {
								logItems[len(logItems)-1]["message"] = logItems[len(logItems)-1]["message"] + "\n" + line
							} else {
								logItems[len(logItems)-1]["message"] = line
							}
						}
					}

					for _, item := range logItems {
						if len(conf.MapJobNameToProperty) > 0 && item[conf.MapJobNameToProperty] != allocation.JobID {
							item[conf.MapJobNameToProperty] = allocation.JobID
						}

						item["type"] = itemType
						item["allocation"] = allocation.ID
						item["job"] = allocation.JobID
						item["group"] = allocation.TaskGroup
						item["task"] = taskName

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
			}

			offsetBytes = offsetBytes + int64(bytes)

			stats := processStats{
				BytesRead:   bytesRead + int64(bytes),
				OffsetBytes: offsetBytes,
				LastSeen:    time.Now(),
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
				log.Error("Error saving log shipping stats to consul: ", err)
				break
			}

			pair = p
		}
	}

	log.Warn(fmt.Sprintf("Loop finished for alloc: %s Task: %s, Type: %s", allocation.ID, taskName, logType))
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

func buildTaskMetaConfig(meta map[string]string) taskMetaConfig {
	config := taskMetaConfig{}

	if value, ok := meta["logzio_type"]; ok {
		config.Type = value
	}

	if value, ok := meta["logzio_delim"]; ok {
		config.Delim = value
	}

	return config
}

func buildMetaConfig(meta map[string]string) metaConfig {
	config := metaConfig{}

	if value, ok := meta["logzio_map_job_name_to_property"]; ok {
		config.MapJobNameToProperty = value
	}

	// Format of "/path/to/file:my_type:delim", Example: "/alloc/logs/app.log:app-logs:\n"
	// The 3rd part is optional. Multiple files can be specified, Example: "/alloc/logs/app.log:app-logs:\n,/alloc/logs/other.log:my-type"
	if value, ok := meta["logzio_log_files"]; ok {
		var logFiles []logFileConfig

		for _, s := range strings.Split(value, ",") {
			parts := strings.Split(s, ":")

			if len(parts) < 0 {
				continue
			}

			logFile := logFileConfig{
				Path:  parts[0],
				Delim: "\n",
				Type:  "nomad-log-file",
			}

			if len(parts) > 1 {
				logFile.Type = parts[1]
			}

			if len(parts) > 2 {
				logFile.Delim = parts[2]
			}

			logFiles = append(logFiles, logFile)
		}

		config.LogFiles = logFiles
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

func filterCancelChannels(channels []chan bool, channel chan bool) []chan bool {
	filtered := []chan bool{}

	for _, c := range channels {
		if channel != c {
			filtered = append(filtered, c)
		}
	}

	return filtered
}
