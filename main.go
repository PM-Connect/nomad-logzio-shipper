package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/pm-connect/nomad-logzio-shipper/allocation"
	"os"
	"regexp"
	"runtime/pprof"
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
	Enabled              bool
}

type taskMetaConfig struct {
	ErrType    string
	ErrDelim   string
	ErrEnabled bool
	OutType    string
	OutDelim   string
	OutEnabled bool
}

type logFileConfig struct {
	Path  string
	Delim string
	Type  string
}

func main() {
	var nomadAddr, nomadClientID, consulAddr, logzToken, logzAddr, consulPath, queueDir string
	var verbose, noSend, profile bool
	var maxAge int

	args := os.Args[1:]

	flags := flag.NewFlagSet("command", flag.ContinueOnError)
	flags.StringVar(&nomadAddr, "nomad", "http://127.0.0.1:4646", "The allocation address to talk to.")
	flags.StringVar(&nomadClientID, "node", "", "The ID of the allocation client/node to scrape logs from.")
	flags.StringVar(&consulAddr, "consul", "127.0.0.1:8500", "The consul address to talk to.")
	flags.StringVar(&logzToken, "logz-token", "", "Your logz.io token.")
	flags.StringVar(&logzAddr, "logz-addr", "https://listener-eu.logz.io:8071", "The logz.io endpoint.")
	flags.BoolVar(&verbose, "verbose", false, "Enable verbose logging.")
	flags.BoolVar(&noSend, "no-send", false, "Do not ship any logs, dry run.")
	flags.BoolVar(&profile, "profile", false, "Profile the cpu usage.")
	flags.IntVar(&maxAge, "max-age", 7, "Set the maximum age in days for allocation log state to be stored in consul for.")
	flags.StringVar(&consulPath, "consul-path", "logzio-nomad", "The KV path in consul to store allocation log state.")
	flags.StringVar(&queueDir, "queue-dir", ".Queue", "The directory to store logzio messages before sending.")

	err := flags.Parse(args)

	if err != nil {
		log.Panic(err)
	}

	args = flags.Args()

	if envToken := os.Getenv("LOGZIO_TOKEN"); logzToken == "" && envToken != "" {
		logzToken = envToken
	} else if logzToken == "" {
		log.Fatal("No logzio token was provided as an argument or in the LOGZIO_TOKEN env variable.")
	}

	if envNomadClientID := os.Getenv("NOMAD_CLIENT_ID"); nomadClientID == "" && envNomadClientID != "" {
		nomadClientID = envNomadClientID
	} else if nomadClientID == "" {
		log.Fatal("No nomad client/node id was provided as an argument or in the NOMAD_CLIENT_ID env variable.")
	}

	if verbose {
		log.SetLevel(log.DebugLevel)
	}

	if profile {
		f, err := os.Create("profile")
		if err != nil {
			log.Fatal(err)
		}

		err = pprof.StartCPUProfile(f)

		if err != nil {
			log.Panic(err)
		}

		go func() {
			time.Sleep(time.Second * 60)
			pprof.StopCPUProfile()
		}()
	}

	log.Info(fmt.Sprintf("Watching allocations for node: %s", nomadClientID))
	log.Info(fmt.Sprintf("Consul Addr: %s Nomad Addr: %s", consulAddr, nomadAddr))

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

	var debugFunc logzio.SenderOptionFunc

	if verbose {
		debugFunc = logzio.SetDebug(os.Stdout)
	} else {
		debugFunc = func(l *logzio.LogzioSender) error {
			return nil
		}
	}

	l, err := logzio.New(
		logzToken,
		logzio.SetUrl(logzAddr),
		logzio.SetDrainDuration(time.Millisecond*500),
		logzio.SetTempDirectory(queueDir),
		logzio.SetDrainDiskThreshold(90),
		debugFunc,
	)

	if err != nil {
		log.Panic(err)
	}

	var currentAllocations []*nomad.Allocation
	addAllocation := make(chan *nomad.Allocation)
	removeAllocation := make(chan *nomad.Allocation)
	allocationSyncErrors := make(chan error)
	allocationCancellation := map[string]chan bool{}

	allocationClient := allocation.Client{
		NomadClient: client,
	}

	go allocationClient.SyncAllocations(&nomadClientID, &currentAllocations, addAllocation, removeAllocation, allocationSyncErrors, allocation.DefaultPollInterval)
	go allocationCleanup(client, kv, consulPath, maxAge)

	exit := make(chan string)

Loop:
	for {
		select {
		case reason := <-exit:
			log.Error(fmt.Sprintf("Exiting: %s", reason))
			break Loop
		case err := <-allocationSyncErrors:
			log.Error(fmt.Sprintf("Error syncing allocations: %s", err))
		case alloc := <-removeAllocation:
			cancelChan, ok := allocationCancellation[alloc.ID]

			if ok {
				cancelChan <- true
			}

			err := purgeAllocationData(alloc, kv, &consulPath)

			if err != nil {
				log.Error(err)
			}

			log.Info(fmt.Sprintf("Removed allocation: %s", alloc.ID))
		case alloc := <-addAllocation:
			log.Info(fmt.Sprintf("Starting for allocation: %s", alloc.ID))

			cancellationChan := make(chan bool)

			allocationCancellation[alloc.ID] = cancellationChan

			go func(alloc *nomad.Allocation) {
				var wg sync.WaitGroup

				var cancelChannels []chan bool
				channels := map[string]chan bool{}

				var allocGroup *nomad.TaskGroup

			GroupListLoop:
				for _, group := range alloc.Job.TaskGroups {
					if *group.Name == alloc.TaskGroup {
						allocGroup = group

						for _, t := range group.Tasks {
							taskConfig := buildTaskMetaConfig(t.Meta)
							if taskConfig.ErrEnabled {
								cancelStderr := make(chan bool)
								cancelChannels = append(cancelChannels, cancelStderr)
								channels[t.Name+"_stderr"] = cancelStderr
							}

							if taskConfig.OutEnabled {
								cancelStdout := make(chan bool)
								cancelChannels = append(cancelChannels, cancelStdout)
								channels[t.Name+"_stdout"] = cancelStdout
							}
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
				for _, group := range alloc.Job.TaskGroups {
					if *group.Name == alloc.TaskGroup {
						for _, task := range group.Tasks {
							taskConfig := buildTaskMetaConfig(task.Meta)

							if taskConfig.ErrEnabled {
								wg.Add(1)
								stopStderr := make(chan struct{})
								go shipLogs(!noSend, allocation.StdErr, conf, &taskConfig, &wg, alloc, task.Name, kv, &allocationClient, &consulPath, stopStderr, filterCancelChannels(cancelChannels, channels[task.Name+"_stderr"]), channels[task.Name+"_stderr"], l, nil)
							}

							if taskConfig.OutEnabled {
								wg.Add(1)
								stopStdout := make(chan struct{})
								go shipLogs(!noSend, allocation.StdOut, conf, &taskConfig, &wg, alloc, task.Name, kv, &allocationClient, &consulPath, stopStdout, filterCancelChannels(cancelChannels, channels[task.Name+"_stdout"]), channels[task.Name+"_stdout"], l, nil)
							}
						}

						break GroupShipLoop
					}
				}

				for i := range conf.LogFiles {
					wg.Add(1)
					stop := make(chan struct{})
					c := channels["file_"+strconv.Itoa(i)]
					go shipLogs(!noSend, "file", conf, nil, &wg, alloc, "leader", kv, &allocationClient, &consulPath, stop, filterCancelChannels(cancelChannels, c), c, l, &conf.LogFiles[i])
				}

				log.Debug(fmt.Sprintf("Waiting on WaitGroup for alloc: %s", alloc.ID))

			StopLoop:
				for {
					select {
					case <-cancellationChan:
						for _, c := range cancelChannels {
							c <- true
						}
						break StopLoop
					}
				}

				wg.Wait()

				log.Warn(fmt.Sprintf("Finished collection for alloc: %s", alloc.ID))
			}(alloc)
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func shipLogs(sendLogs bool, logType string, conf metaConfig, taskConf *taskMetaConfig, wg *sync.WaitGroup, alloc *nomad.Allocation, taskName string, kv *consul.KV, allocClient *allocation.Client, consulPath *string, stopChan chan struct{}, cancelChannels []chan bool, cancel chan bool, l *logzio.LogzioSender, logFile *logFileConfig) {
	defer wg.Done()

	alloc, err := allocClient.GetAllocationInfo(alloc.ID)

	if err != nil {
		log.Error(fmt.Sprintf("Error fetching alloc: %s [%s]", alloc.ID, err))
		for _, c := range cancelChannels {
			c <- true
		}
		return
	}

	if alloc.ClientStatus != "running" {
		log.Error(fmt.Sprintf("Allocation not running: %s", alloc.ID))
		for _, c := range cancelChannels {
			c <- true
		}
		return
	}

	if logFile != nil {
		log.Info(fmt.Sprintf("Shipping Logs: %s %s %s %s", alloc.ID, taskName, logType, logFile.Path))
	} else {
		log.Info(fmt.Sprintf("Shipping Logs: %s %s %s", alloc.ID, taskName, logType))
	}

	var consulStatsKey string

	if logFile == nil {
		consulStatsKey = fmt.Sprintf("%s/%s/%s", alloc.ID, taskName, logType)
	} else {
		consulStatsKey = fmt.Sprintf("%s/_files_/%s/%s", alloc.ID, logType, strings.Replace(logFile.Path, "/", "-", -1))
	}

	pair, _, err := kv.Get(fmt.Sprintf("%s/%s", *consulPath, consulStatsKey), nil)

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
			log.Error(fmt.Sprintf("Error converting consul data to struct for alloc %s: ", alloc.ID), err)
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
		log.Info("Calculating stderr size from log data stream.")
		size := allocClient.GetLogSize(logType, alloc, taskName, offsetBytes)

		if size < offsetBytes || time.Since(time.Unix(0, alloc.CreateTime)).Seconds() <= allocation.DefaultPollInterval {
			offsetBytes = 0
		} else {
			offsetBytes = size
		}

		stream, errors = allocClient.StreamLog(logType, alloc, taskName, offsetBytes, stopChan)

		itemType = "alloc-" + logType

		if taskConf != nil && len(taskConf.ErrType) > 0 {
			itemType = taskConf.ErrType
		}

		delim = "\n"

		if taskConf != nil && len(taskConf.ErrDelim) > 0 {
			delim = taskConf.ErrDelim
		}
	case "stdout":
		log.Info("Calculating stdout size from log data stream.")
		size := allocClient.GetLogSize(logType, alloc, taskName, offsetBytes)

		if size < offsetBytes || time.Since(time.Unix(0, alloc.CreateTime)).Seconds() <= allocation.DefaultPollInterval {
			offsetBytes = 0
		} else {
			offsetBytes = size
		}

		stream, errors = allocClient.StreamLog(logType, alloc, taskName, offsetBytes, stopChan)

		itemType = "alloc-" + logType

		if taskConf != nil && len(taskConf.OutType) > 0 {
			itemType = taskConf.OutType
		}

		delim = "\n"

		if taskConf != nil && len(taskConf.OutDelim) > 0 {
			delim = taskConf.OutDelim
		}
	case "file":
		if logFile == nil {
			log.Error("Attempted to log file with nil logFileConfig.")
			for _, c := range cancelChannels {
				c <- true
			}
			return
		}

		fileNotInitiallyFound := false

		data, err := allocClient.StatFile(alloc, logFile.Path)

	FileExistenceLoop:
		for err != nil {
			if strings.Contains(err.Error(), "no such file or directory") {
				fileNotInitiallyFound = true
				offsetBytes = int64(0)
				log.Warning(fmt.Sprintf("Find not found, 10s retry: %s %s", alloc.ID, logFile.Path))
				alloc, allocErr := allocClient.GetAllocationInfo(alloc.ID)

				if allocErr != nil {
					log.Error("Unable to find alloc: ", allocErr)
					break
				}

				if alloc.ClientStatus != "running" {
					log.Warning(fmt.Sprintf("Allocation is stopped: %s", alloc.ID))
					break
				}

				time.Sleep(time.Second * 10)
				select {
				case <-cancel:
					log.Warn(fmt.Sprintf("Received cancel for alloc: %s Task: %s Type: %s", alloc.ID, taskName, logType))
					break FileExistenceLoop
				default:
					data, err = allocClient.StatFile(alloc, logFile.Path)
				}
			} else {
				break
			}
		}

		if err != nil {
			log.Error("Error calculating file size: ", err)
			for _, c := range cancelChannels {
				c <- true
			}
			return
		}

		if data.Size < offsetBytes || fileNotInitiallyFound || time.Since(time.Unix(0, alloc.CreateTime)).Seconds() <= allocation.DefaultPollInterval {
			offsetBytes = 0
		} else {
			offsetBytes = int64(data.Size)
		}

		stream, errors = allocClient.StreamFile(alloc, logFile.Path, offsetBytes, stopChan)

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
		log.Error("Invalid log type provided.")
		return
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
			log.Warn(fmt.Sprintf("Received cancel for alloc: %s Task: %s Type: %s", alloc.ID, taskName, logType))
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

					logItems := []logItem{{"message": ""}}

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
						if len(conf.MapJobNameToProperty) > 0 && item[conf.MapJobNameToProperty] != alloc.JobID {
							item[conf.MapJobNameToProperty] = alloc.JobID
						}

						item["type"] = itemType
						item["allocation"] = alloc.ID
						item["job"] = alloc.JobID
						item["group"] = alloc.TaskGroup
						item["task"] = taskName

						msg, err := json.Marshal(item)

						if err != nil {
							log.Error(err)
							for _, c := range cancelChannels {
								c <- true
							}
							break
						}

						log.Debug("Sending message.", bytes)

						if sendLogs {
							err = l.Send(msg)

							if err != nil {
								log.Error(err)
							}
						}
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

			p := &consul.KVPair{Key: fmt.Sprintf("%s/%s", *consulPath, consulStatsKey), Value: []byte(statsJSON)}

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

	log.Warn(fmt.Sprintf("Loop finished for alloc: %s Task: %s, Type: %s", alloc.ID, taskName, logType))
}

func purgeAllocationData(alloc *nomad.Allocation, kv *consul.KV, consulPath *string) error {
	path := fmt.Sprintf("%s/%s", *consulPath, alloc.ID)

	_, err := kv.Delete(path, nil)

	return err
}

func allocationCleanup(nomadClient *nomad.Client, kv *consul.KV, consulPath string, maxAge int) {
	nextTime := time.Now().Truncate(time.Hour * 1)
	nextTime = nextTime.Add(time.Hour * 1)
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

	if value, ok := meta["logzio_stderr_type"]; ok {
		config.ErrType = value
	}

	if value, ok := meta["logzio_stderr_delim"]; ok {
		config.ErrDelim = value
	}

	if value, ok := meta["logzio_stderr_enabled"]; ok && value == "false" {
		config.ErrEnabled = false
	} else {
		config.ErrEnabled = true
	}

	if value, ok := meta["logzio_stdout_type"]; ok {
		config.OutType = value
	}

	if value, ok := meta["logzio_stdout_delim"]; ok {
		config.OutDelim = value
	}

	if value, ok := meta["logzio_stdout_enabled"]; ok && value == "false" {
		config.OutEnabled = false
	} else {
		config.OutEnabled = true
	}

	return config
}

func buildMetaConfig(meta map[string]string) metaConfig {
	config := metaConfig{}

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
				Type:  "allocation-log-file",
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

	if value, ok := meta["logzio_enabled"]; ok && value == "false" {
		config.Enabled = false
	} else {
		config.Enabled = true
	}

	return config
}

func filterCancelChannels(channels []chan bool, channel chan bool) []chan bool {
	var filtered []chan bool

	for _, c := range channels {
		if channel != c {
			filtered = append(filtered, c)
		}
	}

	return filtered
}
