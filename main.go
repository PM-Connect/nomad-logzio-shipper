package main

import (
	"encoding/json"
	"fmt"
	"github.com/pm-connect/nomad-logzio-shipper/setup"
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
	"github.com/pm-connect/nomad-logzio-shipper/allocation"
	"github.com/pm-connect/nomad-logzio-shipper/utils"
	log "github.com/sirupsen/logrus"
)

type logItem struct {
	Message    string `json:"message"`
	Type       string `json:"type"`
	Job        string `json:"job"`
	Group      string `json:"group"`
	Task       string `json:"task"`
	Allocation string `json:"alloc"`
}

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

type logShippingConfig struct {
	SendLogs         bool
	LogType          string
	TaskConf         *taskMetaConfig
	WaitGroup        *sync.WaitGroup
	Allocation       *nomad.Allocation
	TaskName         string
	KVStore          *consul.KV
	AllocationClient *allocation.Client
	ConsulPath       *string
	StopChan         chan struct{}
	CancelChannels   []chan bool
	CancelChannel    chan bool
	Logzio           *logzio.LogzioSender
	LogFile          *logFileConfig
	Config           *setup.Config
}

func main() {
	config, err := setup.NewConfig()

	if err != nil {
		log.Panic(err)
	}

	if config.Verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if config.Profile {
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

	log.Infof("Watching allocations for node: %s", config.NomadClientID)
	log.Infof("Consul Addr: %s Nomad Addr: %s", config.ConsulAddr, config.NomadAddr)

	nomadConfig := nomad.Config{
		Address: config.NomadAddr,
	}

	client, err := nomad.NewClient(&nomadConfig)

	if err != nil {
		log.Panic(err)
	}

	consulConfig := consul.DefaultConfig()

	consulConfig.Address = config.ConsulAddr

	consulClient, err := consul.NewClient(consulConfig)

	if err != nil {
		log.Panic(err)
	}

	kv := consulClient.KV()

	var debugFunc logzio.SenderOptionFunc

	if config.Verbose {
		debugFunc = logzio.SetDebug(os.Stdout)
	} else {
		debugFunc = func(l *logzio.LogzioSender) error {
			return nil
		}
	}

	l, err := logzio.New(
		config.LogzIOToken,
		logzio.SetUrl(config.LogzIOAddr),
		logzio.SetDrainDuration(time.Millisecond*500),
		logzio.SetTempDirectory(config.QueueDir),
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

	go allocationClient.SyncAllocations(
		&config.NomadClientID,
		&currentAllocations,
		addAllocation,
		removeAllocation,
		allocationSyncErrors,
		allocation.DefaultPollInterval,
	)
	go allocationCleanup(client, kv, &config.ConsulPath, &config.MaxAge)

	exit := make(chan string)

Loop:
	for {
		select {
		case reason := <-exit:
			log.Errorf("Exiting: %s", reason)
			break Loop
		case err := <-allocationSyncErrors:
			log.Errorf("Error syncing allocations: %s", err)
		case alloc := <-removeAllocation:
			cancelChan, ok := allocationCancellation[alloc.ID]

			if ok {
				cancelChan <- true
			}

			err := purgeAllocationData(alloc, kv, &config.ConsulPath)

			if err != nil {
				log.Error(err)
			}

			log.Infof("[%s] Removed allocation.", alloc.ID)
		case alloc := <-addAllocation:
			log.Infof("[%s] Starting for allocation.", alloc.ID)

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

				var loggingConfigurations []logShippingConfig

			GroupShipLoop:
				for _, group := range alloc.Job.TaskGroups {
					if *group.Name == alloc.TaskGroup {
						for _, task := range group.Tasks {
							taskConfig := buildTaskMetaConfig(task.Meta)

							if taskConfig.ErrEnabled {
								wg.Add(1)
								stopStderr := make(chan struct{})

								loggingConfigurations = append(loggingConfigurations, logShippingConfig{
									SendLogs:         !config.NoSend,
									LogType:          allocation.StdErr,
									TaskConf:         &taskConfig,
									WaitGroup:        &wg,
									Allocation:       alloc,
									TaskName:         task.Name,
									KVStore:          kv,
									AllocationClient: &allocationClient,
									ConsulPath:       &config.ConsulPath,
									StopChan:         stopStderr,
									CancelChannels:   filterCancelChannels(cancelChannels, channels[task.Name+"_stderr"]),
									CancelChannel:    channels[task.Name+"_stderr"],
									Logzio:           l,
									LogFile:          nil,
									Config:           config,
								})
							}

							if taskConfig.OutEnabled {
								wg.Add(1)
								stopStdout := make(chan struct{})

								loggingConfigurations = append(loggingConfigurations, logShippingConfig{
									SendLogs:         !config.NoSend,
									LogType:          allocation.StdOut,
									TaskConf:         &taskConfig,
									WaitGroup:        &wg,
									Allocation:       alloc,
									TaskName:         task.Name,
									KVStore:          kv,
									AllocationClient: &allocationClient,
									ConsulPath:       &config.ConsulPath,
									StopChan:         stopStdout,
									CancelChannels:   filterCancelChannels(cancelChannels, channels[task.Name+"_stdout"]),
									CancelChannel:    channels[task.Name+"_stdout"],
									Logzio:           l,
									LogFile:          nil,
									Config:           config,
								})
							}
						}

						break GroupShipLoop
					}
				}

				for i := range conf.LogFiles {
					wg.Add(1)
					stop := make(chan struct{})
					c := channels["file_"+strconv.Itoa(i)]

					loggingConfigurations = append(loggingConfigurations, logShippingConfig{
						SendLogs:         !config.NoSend,
						LogType:          "file",
						TaskConf:         nil,
						WaitGroup:        &wg,
						Allocation:       alloc,
						TaskName:         "leader",
						KVStore:          kv,
						AllocationClient: &allocationClient,
						ConsulPath:       &config.ConsulPath,
						StopChan:         stop,
						CancelChannels:   filterCancelChannels(cancelChannels, c),
						CancelChannel:    c,
						Logzio:           l,
						LogFile:          &conf.LogFiles[i],
						Config:           config,
					})
				}

				for i, loggingConfiguration := range loggingConfigurations {
					wg.Add(1)
					go shipLogs(i, loggingConfiguration)
				}

				log.Infof("[%s] Waiting on WaitGroup for alloc.", alloc.ID)

			StopLoop:
				for {
					select {
					case <-cancellationChan:
						triggerCancel(cancelChannels)
						break StopLoop
					}
				}

				wg.Wait()

				log.Warnf("[%s] Finished collection for alloc.", alloc.ID)
			}(alloc)
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func shipLogs(workerId int, conf logShippingConfig) {
	defer conf.WaitGroup.Done()

	alloc, err := conf.AllocationClient.GetAllocationInfo(conf.Allocation.ID)

	if err != nil {
		log.Errorf("[%d:%s@%s] Error fetching alloc: [%s]", workerId, conf.LogType, alloc.ID, err)
		triggerCancel(conf.CancelChannels)
		return
	}

	if alloc.ClientStatus != "running" {
		log.Errorf("[%d:%s@%s] Allocation not running", workerId, conf.LogType, alloc.ID)
		triggerCancel(conf.CancelChannels)
		return
	}

	if conf.LogFile != nil {
		log.Infof("[%d:%s@%s] Shipping Logs for task '%s' type '%s' path  '%s'", workerId, conf.LogType, alloc.ID, conf.TaskName, conf.LogType, conf.LogFile.Path)
	} else {
		if conf.TaskConf != nil {
			if conf.LogType == "stderr" && len(conf.TaskConf.ErrType) > 0 {
				log.Infof("[%d:%s@%s] Shipping Logs for task '%s' type '%s' to type '%s'", workerId, conf.LogType, alloc.ID, conf.TaskName, conf.LogType, conf.TaskConf.ErrType)
			} else if conf.LogType == "stdout" && len(conf.TaskConf.OutType) > 0 {
				log.Infof("[%d:%s@%s] Shipping Logs for task '%s' type '%s' to type '%s'", workerId, conf.LogType, alloc.ID, conf.TaskName, conf.LogType, conf.TaskConf.OutType)
			}
		} else {
			log.Infof("[%d:%s@%s] Shipping Logs for task '%s' type '%s'", workerId, conf.LogType, alloc.ID, conf.TaskName, conf.LogType)
		}
	}

	var consulStatsKey string

	if conf.LogFile == nil {
		consulStatsKey = fmt.Sprintf("%s/%s/%s", alloc.ID, conf.TaskName, conf.LogType)
	} else {
		consulStatsKey = fmt.Sprintf(
			"%s/_files_/%s/%s",
			alloc.ID,
			conf.LogType,
			strings.Replace(conf.LogFile.Path, "/", "-", -1),
		)
	}

	pair, _, err := conf.KVStore.Get(fmt.Sprintf("%s/%s", *conf.ConsulPath, consulStatsKey), nil)

	if err != nil {
		log.Errorf("[%d:%s@%s] Error fetching consul log shipping stats: %s", workerId, conf.LogType, alloc.ID, err)
		triggerCancel(conf.CancelChannels)
		return
	}

	var offsetBytes int64
	var bytesRead int64
	var stats processStats

	if pair != nil {
		err := json.Unmarshal(pair.Value, &stats)

		if err != nil {
			log.Errorf("[%d:%s@%s] Error converting consul data to struct: %s", workerId, conf.LogType, alloc.ID, err)
			triggerCancel(conf.CancelChannels)
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

	switch conf.LogType {
	case "file":
		if conf.LogFile == nil {
			log.Errorf("[%d:%s@%s] Attempted to log file with nil logFileConfig.", alloc.ID)
			triggerCancel(conf.CancelChannels)
			return
		}

		fileNotInitiallyFound := false

		data, err := conf.AllocationClient.StatFile(alloc, conf.LogFile.Path)

		for err != nil {
			if strings.Contains(err.Error(), "no such file or directory") {
				fileNotInitiallyFound = true
				offsetBytes = int64(0)
				log.Warningf("[%d:%s@%s] File not found, 10s retry: %s %s", workerId, conf.LogType, alloc.ID, alloc.Name, conf.LogFile.Path)
				alloc, allocErr := conf.AllocationClient.GetAllocationInfo(alloc.ID)

				if allocErr != nil {
					log.Errorf("[%d:%s@%s] Unable to find alloc: %s", workerId, conf.LogType, alloc.ID, allocErr)
					triggerCancel(conf.CancelChannels)
					return
				}

				if alloc.ClientStatus != "running" {
					log.Warningf("[%d:%s@%s] Allocation is %s", workerId, conf.LogType, alloc.ID, alloc.ClientStatus)
					triggerCancel(conf.CancelChannels)
					return
				}

				time.Sleep(time.Second * 10)
				select {
				case <-conf.CancelChannel:
					log.Warnf(
						"[%d:%s@%s] Received cancel for Task: %s",
						workerId,
						conf.LogType,
						alloc.ID,
						conf.TaskName,
					)
					log.Warnf("[%d:%s@%s] Loop finished for Task: %s, Type: %s", workerId, conf.LogType, alloc.ID, conf.TaskName, conf.LogType)
					return
				default:
					data, err = conf.AllocationClient.StatFile(alloc, conf.LogFile.Path)
				}
			} else {
				log.Errorf("[%d:%s@%s] Error calculating file size: %s", workerId, conf.LogType, alloc.ID, err)
				triggerCancel(conf.CancelChannels)
				return
			}
		}

		if data.Size < offsetBytes || fileNotInitiallyFound || time.Since(time.Unix(0, alloc.CreateTime)).Seconds() <= allocation.DefaultPollInterval {
			if data.Size < offsetBytes {
				log.Warnf("[%d:%s@%s] Offset greater than total available data, got offset %d expected less than or equal to %d.", workerId, conf.LogType, alloc.ID, offsetBytes, data.Size)
			}
			offsetBytes = 0
		} else {
			offsetBytes = int64(data.Size)
		}

		log.Infof("[%d:%s@%s] Streaming logs for path %s from offset: %d", workerId, conf.LogType, alloc.ID, conf.LogFile.Path, offsetBytes)

		stream, errors = conf.AllocationClient.StreamFile(alloc, conf.LogFile.Path, offsetBytes, conf.StopChan)

		if len(conf.LogFile.Type) == 0 {
			log.Errorf("[%d:%s@%s] Log file type must be set.", workerId, conf.LogType, alloc.ID)
			triggerCancel(conf.CancelChannels)
			return
		}

		itemType = conf.LogFile.Type

		if len(conf.LogFile.Delim) > 0 {
			delim = conf.LogFile.Delim
		}
	case "stderr", "stdout":
		log.Infof("[%d:%s@%s] Calculating size from log data stream.", workerId, conf.LogType, alloc.ID)
		size := conf.AllocationClient.GetLogSize(conf.LogType, alloc, conf.TaskName, 0)

		if size < offsetBytes || time.Since(time.Unix(0, alloc.CreateTime)).Seconds() <= allocation.DefaultPollInterval {
			if size < offsetBytes {
				log.Warnf("[%d:%s@%s] Offset greater than total available data, got offset \"%d\" expected less than or equal to \"%d\"", workerId, conf.LogType, alloc.ID, offsetBytes, size)
			}
			offsetBytes = 0
		} else {
			offsetBytes = size
		}

		log.Infof("[%d:%s@%s] Streaming logs from offset: %d", workerId, conf.LogType, alloc.ID, offsetBytes)

		stream, errors = conf.AllocationClient.StreamLog(conf.LogType, alloc, conf.TaskName, offsetBytes, conf.StopChan)

		itemType = "nomad-" + conf.LogType

		if conf.TaskConf != nil && ((len(conf.TaskConf.ErrType) > 0 && conf.LogType == "stderr") || (len(conf.TaskConf.OutType) > 0 && conf.LogType == "stdout")) {
			switch conf.LogType {
			case "stderr":
				itemType = conf.TaskConf.ErrType
			case "stdout":
				itemType = conf.TaskConf.OutType
			}
		}

		delim = "\n"

		if conf.TaskConf != nil && ((len(conf.TaskConf.ErrDelim) > 0 && conf.LogType == "stderr") || (len(conf.TaskConf.OutDelim) > 0 && conf.LogType == "stdout")) {
			switch conf.LogType {
			case "stderr":
				delim = conf.TaskConf.ErrDelim
			case "stdout":
				delim = conf.TaskConf.OutDelim
			}
		}
	default:
		log.Errorf("[%d:%s@%s] Invalid log type provided.", alloc.ID)
		return
	}

StreamLoop:
	for {
		select {
		case err := <-errors:
			if strings.Contains(err.Error(), "no such file or directory") {
				log.Warningf("[%d:%s@%s] Unable to find file: %s", workerId, conf.LogType, alloc.ID, err)
			} else {
				log.Errorf("[%d:%s@%s] Error while streaming: %s", workerId, conf.LogType, alloc.ID, err)
			}

			triggerCancel(conf.CancelChannels)

			break StreamLoop
		case <-conf.CancelChannel:
			log.Warnf(
				"[%d:%s@%s] Received cancel for Task: %s Type: %s",
				alloc.ID,
				conf.TaskName,
				conf.LogType,
			)
			conf.StopChan <- struct{}{}
			break StreamLoop
		case data, ok := <-stream:
			if !ok {
				log.Errorf(
					"[%d:%s@%s] Not ok when reading from stream: Task: %s Type: %s",
					alloc.ID,
					conf.TaskName,
					conf.LogType,
				)

				triggerCancel(conf.CancelChannels)

				break StreamLoop
			}

			var bytes int

			if len(data.FileEvent) > 0 {
				log.Infof(
					"[%d:%s@%s] Resetting offset due to file event: %s Task: %s Type: %s",
					workerId,
					alloc.ID,
					data.FileEvent,
					conf.TaskName,
					conf.LogType,
				)

				triggerCancel(conf.CancelChannels)

				break StreamLoop
			} else {
				bytes = len(data.Data)

				if alloc.ID != conf.Config.SelfAlloc && len(conf.Config.SelfAlloc) > 0 {
					log.Debugf("[%d:%s@%s] Processing %d bytes", workerId, conf.LogType, alloc.ID, bytes)
				}

				if offsetBytes > 0 && pair != nil {
					value := string(data.Data)

					logItems := []logItem{{Message: ""}}

					reg, err := regexp.Compile(delim)

					if err != nil {
						log.Errorf("[%d:%s@%s] Error compiling regex: %s", workerId, conf.LogType, alloc.ID, err)
						triggerCancel(conf.CancelChannels)
						break
					}

					for _, line := range strings.Split(value, "\n") {
						if len(strings.TrimSpace(line)) == 0 {
							continue
						}

						if (reg.MatchString(line) || delim == "\n") && len(logItems[len(logItems)-1].Message) > 0 {
							logItems = append(logItems, logItem{
								Message: line,
							})
						} else {
							if len(logItems[len(logItems)-1].Message) > 0 {
								logItems[len(logItems)-1].Message = logItems[len(logItems)-1].Message + "\n" + line
							} else {
								logItems[len(logItems)-1].Message = line
							}
						}
					}

					if alloc.ID != conf.Config.SelfAlloc && len(conf.Config.SelfAlloc) > 0 {
						log.Debugf("[%d:%s@%s] Found log items: %d", workerId, conf.LogType, alloc.ID, len(logItems))
					}

					sentBytes := 0
					sentMessages := 0

					for _, item := range logItems {
						item.Type = itemType
						item.Allocation = alloc.ID
						item.Job = alloc.JobID
						item.Group = alloc.TaskGroup
						item.Task = conf.TaskName

						msg, err := json.Marshal(item)

						if err != nil {
							log.Error(err)
							triggerCancel(conf.CancelChannels)
							break
						}

						if alloc.ID != conf.Config.SelfAlloc && len(conf.Config.SelfAlloc) > 0 {
							log.Debugf("[%d:%s@%s] Sending message with bytes: %d", workerId, conf.LogType, alloc.ID, len(item.Message))
						}

						if conf.SendLogs {
							err = conf.Logzio.Send(msg)

							if err != nil {
								log.Error(err)
							} else {
								sentBytes = sentBytes + len(item.Message)
								sentMessages = sentMessages + 1
							}
						} else {
							sentBytes = sentBytes + len(item.Message)
							sentMessages = sentMessages + 1
						}
					}

					if alloc.ID != conf.Config.SelfAlloc && len(conf.Config.SelfAlloc) > 0 {
						log.Debugf("[%d:%s@%s] Sent %d/%d messages with %d/%d bytes", workerId, conf.LogType, alloc.ID, sentMessages, len(logItems), sentBytes, bytes)
					}
				}
			}

			offsetBytes = offsetBytes + int64(bytes)
			bytesRead = bytesRead + int64(bytes)

			if offsetBytes > bytesRead {
				log.Warnf("[%d:%s@%s] Detected offset greater than total bytes read, offset %d, bytes read %d.", workerId, conf.LogType, alloc.ID, offsetBytes, bytesRead)
				offsetBytes = bytesRead
			}

			stats := processStats{
				BytesRead:   bytesRead,
				OffsetBytes: offsetBytes,
				LastSeen:    time.Now(),
			}

			statsJSON, err := json.Marshal(stats)

			if err != nil {
				triggerCancel(conf.CancelChannels)
				log.Error(err)
				break
			}

			p := &consul.KVPair{Key: fmt.Sprintf("%s/%s", *conf.ConsulPath, consulStatsKey), Value: []byte(statsJSON)}

			_, err = conf.KVStore.Put(p, nil)

			if err != nil {
				triggerCancel(conf.CancelChannels)
				log.Errorf("[%d:%s@%s] Error saving log shipping stats to consul: %s", workerId, conf.LogType, alloc.ID, err)
				break
			}

			pair = p
		}
	}

	log.Warnf("[%d:%s@%s] Loop finished for Task: %s, Type: %s", workerId, conf.LogType, alloc.ID, conf.TaskName, conf.LogType)
}

func purgeAllocationData(alloc *nomad.Allocation, kv *consul.KV, consulPath *string) error {
	path := fmt.Sprintf("%s/%s", *consulPath, alloc.ID)

	_, err := kv.Delete(path, nil)

	return err
}

func allocationCleanup(nomadClient *nomad.Client, kv *consul.KV, consulPath *string, maxAge *int) {
	utils.WaitUntil(time.Hour * 1)

	pairs, _, err := kv.List(*consulPath, nil)

	if err != nil {
		log.Error(err)
		allocationCleanup(nomadClient, kv, consulPath, maxAge)
		return
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

		if int(age) >= *maxAge {
			log.Infof("Deleting Key: %s", pair.Key)

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

func triggerCancel(channels []chan bool) {
	for _, c := range channels {
		c <- true
	}
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
