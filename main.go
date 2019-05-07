package main

import (
	"encoding/json"
	"fmt"
	"github.com/pm-connect/nomad-logzio-shipper/setup"
	"github.com/pm-connect/nomad-logzio-shipper/statsd"
	"net/http"
	"os"
	"regexp"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	consul "github.com/hashicorp/consul/api"
	nomad "github.com/hashicorp/nomad/api"
	"github.com/lithammer/shortuuid"
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
	DisplayName      string
	TaskConf         *taskMetaConfig
	WaitGroup        *sync.WaitGroup
	Allocation       nomad.Allocation
	TaskName         string
	KVStore          *consul.KV
	AllocationClient *allocation.Client
	ConsulPath       *string
	StopChan         chan struct{}
	CancelChannels   *[]chan bool
	CancelChannel    chan bool
	Logzio           *logzio.LogzioSender
	LogFile          *logFileConfig
	Config           *setup.Config
}

type Metric struct {
	Name  string
	Value int
}

type MetricStore map[string]int

func main() {
	config, err := setup.NewConfig()

	logger := log.New()

	if err != nil {
		log.Panic(err)
	}

	if config.Debug {
		log.SetLevel(log.DebugLevel)
		logger.SetLevel(log.DebugLevel)
	} else if config.Verbose {
		log.SetLevel(log.InfoLevel)
		logger.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(log.WarnLevel)
		logger.SetLevel(log.WarnLevel)
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

	if config.LogzioDebug {
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

	metrics := make(chan Metric)

	var metricHandlers []func(Metric)

	storedMetricsMutex := &sync.Mutex{}
	storedMetrics := MetricStore{}

	if len(config.StatsdHost) > 0 && config.StatsdPort > 0 {
		statsdClient := statsd.New(config.StatsdHost, config.StatsdPort)

		metricHandlers = append(metricHandlers, func(metric Metric) {
			statsdClient.IncrementByValue(metric.Name, metric.Value)
		})
	}

	timedMetrics := map[int64]MetricStore{}
	timedMetricsMutex := &sync.Mutex{}

	if config.UI {
		metricHandlers = append(metricHandlers, func(metric Metric) {
			storedMetricsMutex.Lock()
			if existing, ok := storedMetrics[metric.Name]; ok {
				storedMetrics[metric.Name] = existing + metric.Value
			} else {
				storedMetrics[metric.Name] = metric.Value
			}
			storedMetricsMutex.Unlock()

			timestamp := time.Now().Unix()

			for i := range timedMetrics {
				t := time.Unix(i, 0)

				if time.Since(t).Seconds() > 3600 {
					delete(timedMetrics, i)
				}
			}

			timedMetricsMutex.Lock()

			if _, ok := timedMetrics[timestamp]; !ok {
				timedMetrics[timestamp] = MetricStore{}
			}

			if existing, ok := timedMetrics[timestamp][metric.Name]; ok {
				timedMetrics[timestamp][metric.Name] = metric.Value + existing
			} else {
				timedMetrics[timestamp][metric.Name] = metric.Value
			}

			timedMetricsMutex.Unlock()
		})
	}

	go metricListener(metrics, metricHandlers)

	currentAllocationsMutex := &sync.Mutex{}

	var currentAllocations []nomad.Allocation
	addAllocation := make(chan nomad.Allocation)
	removeAllocation := make(chan nomad.Allocation)
	allocationSyncErrors := make(chan error)
	cancellationChannel := make(chan nomad.Allocation)

	allocationClient := allocation.Client{
		NomadClient: client,
	}

	go allocationClient.SyncAllocations(
		&config.NomadClientID,
		&currentAllocations,
		addAllocation,
		removeAllocation,
		allocationSyncErrors,
		currentAllocationsMutex,
		allocation.DefaultPollInterval,
		logger,
	)
	go allocationCleanup(client, kv, &config.ConsulPath, &config.MaxAge)

	exit := make(chan string)

	allocCancellationChannels := map[string]*[]chan bool{}
	allocCancellationChannelsMutex := &sync.Mutex{}

	allocationWorkers := map[string][]string{}
	allocationWorkersMutex := &sync.Mutex{}

	if config.UI {
		go runUi(config, &currentAllocations, &storedMetrics, &allocationWorkers, &timedMetrics, currentAllocationsMutex, allocationWorkersMutex, timedMetricsMutex, storedMetricsMutex)
	}

	go func() {
		for {
			alloc := <-cancellationChannel

			log.Infof("[%s] Received cancel for allocation.", alloc.ID)

			currentAllocationsMutex.Lock()
			currentAllocations = filterAllocationsExclude(currentAllocations, alloc.ID)
			currentAllocationsMutex.Unlock()

			channels := allocCancellationChannels[alloc.ID]

			allocationWorkersMutex.Lock()
			delete(allocationWorkers, alloc.ID)
			allocationWorkersMutex.Unlock()

			allocCancellationChannelsMutex.Lock()
			if channels != nil && len(*channels) > 0 {
				for _, c := range *channels {
					select {
					case c <- true:
					default:
					}
				}
			}
			allocCancellationChannelsMutex.Unlock()

			incrementMetric(metrics, fmt.Sprintf("%slogshipper_allocation_cancellations", config.StatsdPrefix), 1)
		}
	}()

Loop:
	for {
		select {
		case reason := <-exit:
			log.Errorf("Exiting: %s", reason)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_exits", config.StatsdPrefix), 1)
			break Loop
		case err := <-allocationSyncErrors:
			log.Errorf("Error syncing allocations: %s", err)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_allocation_sync_errors", config.StatsdPrefix), 1)
		case alloc := <-removeAllocation:
			log.Infof("[%s] Received allocation removal request.", alloc.ID)

			currentAllocationsMutex.Lock()
			currentAllocations = filterAllocationsExclude(currentAllocations, alloc.ID)
			currentAllocationsMutex.Unlock()

			channels := allocCancellationChannels[alloc.ID]

			allocationWorkersMutex.Lock()
			delete(allocationWorkers, alloc.ID)
			allocationWorkersMutex.Unlock()

			allocCancellationChannelsMutex.Lock()
			if channels != nil && len(*channels) > 0 {
				for _, c := range *channels {
					select {
					case c <- true:
					default:
					}
				}
			}
			allocCancellationChannelsMutex.Unlock()

			log.Infof("[%s] Purging stored allocation data.", alloc.ID)
			err := purgeAllocationData(&alloc, kv, &config.ConsulPath)

			if err != nil {
				log.Error(err)
			}

			allocCancellationChannelsMutex.Lock()
			delete(allocCancellationChannels, alloc.ID)
			allocCancellationChannelsMutex.Unlock()

			log.Infof("[%s] Removed allocation.", alloc.ID)

			incrementMetric(metrics, fmt.Sprintf("%slogshipper_allocation_removals", config.StatsdPrefix), 1)
		case alloc := <-addAllocation:
			currentAlloc := alloc

			log.Infof("[%s] Starting for allocation.", currentAlloc.ID)

			incrementMetric(metrics, fmt.Sprintf("%slogshipper_allocation_received", config.StatsdPrefix), 1)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_allocation_received_for_alloc_%s", config.StatsdPrefix, alloc.ID), 1)

			go func(alloc nomad.Allocation) {
				defer func() {
					if err := recover(); err != nil {
						log.Fatalf("[%s] Critical Error: %s", alloc.ID, err)
					}
				}()

				log.Infof("[%s] Starting shipping.", alloc.ID)

				var wg sync.WaitGroup
				var allocGroup *nomad.TaskGroup
				var loggingConfigurations []logShippingConfig
				var cancellations []chan bool

			GroupListLoop:
				for _, group := range alloc.Job.TaskGroups {
					if *group.Name == alloc.TaskGroup {
						allocGroup = group
						break GroupListLoop
					}
				}

				if allocGroup == nil {
					log.Errorf("[%s] Unable to find group for allocation.", alloc.ID)
					incrementMetric(metrics, fmt.Sprintf("%slogshipper_allocation_errors", config.StatsdPrefix), 1)
				}

				conf := buildMetaConfig(allocGroup.Meta)

			GroupShipLoop:
				for _, group := range alloc.Job.TaskGroups {
					if *group.Name == alloc.TaskGroup {
						for _, task := range group.Tasks {
							taskConfig := buildTaskMetaConfig(task.Meta)

							if taskConfig.ErrEnabled {
								cancel := make(chan bool)

								cancellations = append(cancellations, cancel)

								loggingConfigurations = append(loggingConfigurations, logShippingConfig{
									SendLogs:         !config.NoSend,
									LogType:          allocation.StdErr,
									TaskConf:         &taskConfig,
									WaitGroup:        &wg,
									Allocation:       alloc,
									DisplayName:      fmt.Sprintf("%s/%s/%s", *alloc.Job.Name, *group.Name, task.Name),
									TaskName:         task.Name,
									KVStore:          kv,
									AllocationClient: &allocationClient,
									ConsulPath:       &config.ConsulPath,
									CancelChannels:   &cancellations,
									CancelChannel:    cancel,
									Logzio:           l,
									LogFile:          nil,
									Config:           config,
								})
							}

							if taskConfig.OutEnabled {
								cancel := make(chan bool)

								cancellations = append(cancellations, cancel)

								loggingConfigurations = append(loggingConfigurations, logShippingConfig{
									SendLogs:         !config.NoSend,
									LogType:          allocation.StdOut,
									TaskConf:         &taskConfig,
									WaitGroup:        &wg,
									Allocation:       alloc,
									DisplayName:      fmt.Sprintf("%s/%s/%s", *alloc.Job.Name, *group.Name, task.Name),
									TaskName:         task.Name,
									KVStore:          kv,
									AllocationClient: &allocationClient,
									ConsulPath:       &config.ConsulPath,
									CancelChannels:   &cancellations,
									CancelChannel:    cancel,
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
					cancel := make(chan bool)

					cancellations = append(cancellations, cancel)

					logFileConf := conf.LogFiles[i]

					loggingConfigurations = append(loggingConfigurations, logShippingConfig{
						SendLogs:         !config.NoSend,
						LogType:          "file",
						TaskConf:         nil,
						WaitGroup:        &wg,
						Allocation:       alloc,
						DisplayName:      fmt.Sprintf("%s/%s/leader", *alloc.Job.Name, alloc.Name),
						TaskName:         "leader",
						KVStore:          kv,
						AllocationClient: &allocationClient,
						ConsulPath:       &config.ConsulPath,
						CancelChannels:   &cancellations,
						CancelChannel:    cancel,
						Logzio:           l,
						LogFile:          &logFileConf,
						Config:           config,
					})
				}

				log.Infof("[%s] Creating total of %d workers.", alloc.ID, len(loggingConfigurations))

				incrementMetric(metrics, fmt.Sprintf("%slogshipper_workers_created", config.StatsdPrefix), len(loggingConfigurations))

				wg.Add(len(loggingConfigurations))

				for _, loggingConfiguration := range loggingConfigurations {
					id := shortuuid.New()
					config := loggingConfiguration
					log.Infof("[%s:%s@%s] Starting task", id, config.LogType, alloc.ID)
					go func(conf logShippingConfig) {
						defer func() {
							defer wg.Done()

							if err := recover(); err != nil {
								log.Errorf("[%s] Critical Error: %s", alloc.ID, err)
								triggerCancel(conf.CancelChannels)
							}
						}()

						shipLogs(id, config, metrics)
					}(config)

					allocationWorkersMutex.Lock()
					filePath := ""

					if config.LogFile != nil {
						filePath = config.LogFile.Path
					}

					allocationWorkers[alloc.ID] = append(allocationWorkers[alloc.ID], fmt.Sprintf("%s %s %s %s", id, config.DisplayName, config.LogType, filePath))
					allocationWorkersMutex.Unlock()
				}

				allocCancellationChannelsMutex.Lock()
				allocCancellationChannels[alloc.ID] = &cancellations
				allocCancellationChannelsMutex.Unlock()

				log.Infof("[%s] Waiting on WaitGroup for alloc.", alloc.ID)

				wg.Wait()

				a, _ := allocationClient.GetAllocationInfo(alloc.ID)

				expectedRestart := false

				if a != nil && (a.ClientStatus == "running" || a.ClientStatus == "restarting") {
					expectedRestart = true
					incrementMetric(metrics, fmt.Sprintf("%slogshipper_expected_restarts", config.StatsdPrefix), 1)
					incrementMetric(metrics, fmt.Sprintf("%slogshipper_expected_restarts_for_alloc_%s", config.StatsdPrefix, alloc.ID), 1)
				}

				log.Warnf("[%s] Finished collection for alloc. Expected Restart: %t", alloc.ID, expectedRestart)

				incrementMetric(metrics, fmt.Sprintf("%slogshipper_allocation_collections_completed", config.StatsdPrefix), 1)

				cancellationChannel <- alloc
			}(currentAlloc)
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func runUi(config *setup.Config, currentAllocations *[]nomad.Allocation, metrics *MetricStore, workers *map[string][]string, timedMetrics *map[int64]MetricStore, currentAllocsMutex *sync.Mutex, workersMutex *sync.Mutex, timedMutex *sync.Mutex, metricMutex *sync.Mutex) {
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		workersMutex.Lock()
		allWorkers := *workers
		workersMutex.Unlock()

		_, _ = writer.Write([]byte(fmt.Sprintf("Nomad Node: %s\n", config.NomadClientID)))
		_, _ = writer.Write([]byte(fmt.Sprintf("Shipper Alloc: %s\n", config.SelfAlloc)))

		_, _ = writer.Write([]byte("\n"))

		_, _ = writer.Write([]byte("Watching Allocations:\n"))

		currentAllocsMutex.Lock()
		allocs := *currentAllocations
		currentAllocsMutex.Unlock()

		for _, alloc := range allocs {
			_, _ = writer.Write([]byte(alloc.ID + " " + *alloc.Job.Name + "\n"))

			if allocWorkers, ok := allWorkers[alloc.ID]; ok {
				for _, name := range allocWorkers {
					_, _ = writer.Write([]byte("    " + name + "\n"))
				}
			}

			_, _ = writer.Write([]byte("\n"))
		}

		_, _ = writer.Write([]byte("\n"))
		_, _ = writer.Write([]byte("\n"))

		_, _ = writer.Write([]byte("Metrics Averages Over 12 Hours:\n"))
		metricMutex.Lock()
		timedMutex.Lock()
		allMetrics := *metrics
		allTimedMetrics := *timedMetrics
		for metric := range allMetrics {
			sum := 0

			for _, items := range allTimedMetrics {
				if value, ok := items[metric]; ok {
					sum = sum + value
				}
			}

			_, _ = writer.Write([]byte(fmt.Sprintf("%s %d\n", metric, sum / len(allTimedMetrics))))
		}

		metricMutex.Unlock()
		timedMutex.Unlock()

		_, _ = writer.Write([]byte("\n"))
		_, _ = writer.Write([]byte("\n"))

		_, _ = writer.Write([]byte("Metrics:\n"))
		metricMutex.Lock()
		for metric, value := range allMetrics {
			_, _ = writer.Write([]byte(fmt.Sprintf("%s %d\n", metric, value)))
		}
		metricMutex.Unlock()
	})

	if err := http.ListenAndServe(fmt.Sprintf(":%d", config.UIPort), nil); err != nil {
		log.Fatal(err)
	}
}

func shipLogs(workerId string, conf logShippingConfig, metrics chan<- Metric) {
	incrementMetric(metrics, fmt.Sprintf("%slogshipper_workers_started", conf.Config.StatsdPrefix), 1)
	incrementMetric(metrics, fmt.Sprintf("%slogshipper_workers_started_for_type_%s", conf.Config.StatsdPrefix, conf.LogType), 1)

	log.Infof("[%s:%s@%s] Starting worker", workerId, conf.LogType, conf.Allocation.ID)

	alloc, err := conf.AllocationClient.GetAllocationInfo(conf.Allocation.ID)

	if err != nil {
		log.Errorf("[%s:%s@%s] Error fetching alloc: [%s]", workerId, conf.LogType, alloc.ID, err)
		triggerCancel(conf.CancelChannels)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), 1)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
		return
	}

	if alloc.ClientStatus != "running" {
		log.Errorf("[%s:%s@%s] Allocation not running", workerId, conf.LogType, alloc.ID)
		triggerCancel(conf.CancelChannels)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
		return
	}

	if conf.LogFile != nil {
		log.Infof("[%s:%s@%s] Shipping Logs for task '%s' type '%s' path  '%s'", workerId, conf.LogType, alloc.ID, conf.DisplayName, conf.LogType, conf.LogFile.Path)
	} else {
		if conf.TaskConf != nil {
			if conf.LogType == "stderr" && len(conf.TaskConf.ErrType) > 0 {
				log.Infof("[%s:%s@%s] Shipping Logs for task '%s' type '%s' to type '%s'", workerId, conf.LogType, alloc.ID, conf.DisplayName, conf.LogType, conf.TaskConf.ErrType)
			} else if conf.LogType == "stdout" && len(conf.TaskConf.OutType) > 0 {
				log.Infof("[%s:%s@%s] Shipping Logs for task '%s' type '%s' to type '%s'", workerId, conf.LogType, alloc.ID, conf.DisplayName, conf.LogType, conf.TaskConf.OutType)
			} else {
				log.Infof("[%s:%s@%s] Shipping Logs for task '%s' type '%s'", workerId, conf.LogType, alloc.ID, conf.DisplayName, conf.LogType)
			}
		} else {
			log.Infof("[%s:%s@%s] Shipping Logs for task '%s' type '%s'", workerId, conf.LogType, alloc.ID, conf.DisplayName, conf.LogType)
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
		log.Errorf("[%s:%s@%s] Error fetching consul log shipping stats: %s", workerId, conf.LogType, alloc.ID, err)
		triggerCancel(conf.CancelChannels)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
		return
	}

	var offsetBytes int64
	var bytesRead int64
	var stats processStats

	if pair != nil {
		err := json.Unmarshal(pair.Value, &stats)

		if err != nil {
			log.Errorf("[%s:%s@%s] Error converting consul data to struct: %s", workerId, conf.LogType, alloc.ID, err)
			triggerCancel(conf.CancelChannels)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
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
	var streamStop chan struct{}

	defer close(streamStop)

	switch conf.LogType {
	case "file":
		if conf.LogFile == nil {
			log.Errorf("[%s:%s@%s] Attempted to log file with nil logFileConfig.", workerId, conf.LogType, alloc.ID)
			triggerCancel(conf.CancelChannels)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
			return
		}

		fileNotInitiallyFound := false

		data, err := conf.AllocationClient.StatFile(alloc, conf.LogFile.Path)

		for err != nil {
			if strings.Contains(err.Error(), "no such file or directory") {
				fileNotInitiallyFound = true
				log.Debugf("[%s:%s@%s] File not found, 10s retry: %s %s", workerId, conf.LogType, alloc.ID, alloc.Name, conf.LogFile.Path)

				incrementMetric(metrics, fmt.Sprintf("%slogshipper_files_not_found", conf.Config.StatsdPrefix), 1)

				alloc, allocErr := conf.AllocationClient.GetAllocationInfo(alloc.ID)

				if allocErr != nil {
					log.Errorf("[%s:%s@%s] Unable to find alloc: %s", workerId, conf.LogType, alloc.ID, allocErr)
					triggerCancel(conf.CancelChannels)
					incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
					incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
					incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
					incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
					return
				}

				if alloc.ClientStatus != "running" {
					log.Warningf("[%s:%s@%s] Allocation is %s", workerId, conf.LogType, alloc.ID, alloc.ClientStatus)
					triggerCancel(conf.CancelChannels)
					incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
					incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_warnings", conf.Config.StatsdPrefix), 1)
					return
				}

				time.Sleep(time.Second * 10)
				select {
				case <-conf.CancelChannel:
					log.Warnf(
						"[%s:%s@%s] Received cancel for Task: %s",
						workerId,
						conf.LogType,
						alloc.ID,
						conf.DisplayName,
					)
					log.Warnf("[%s:%s@%s] Loop finished for Task: %s, Type: %s", workerId, conf.LogType, alloc.ID, conf.DisplayName, conf.LogType)
					incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_warnings", conf.Config.StatsdPrefix), 2)
					incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_received", conf.Config.StatsdPrefix), 1)
					return
				default:
					data, err = conf.AllocationClient.StatFile(alloc, conf.LogFile.Path)
				}
			} else {
				log.Errorf("[%s:%s@%s] Error calculating file size: %s", workerId, conf.LogType, alloc.ID, err)
				triggerCancel(conf.CancelChannels)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
				return
			}
		}

		log.Infof("[%s:%s@%s] Calculated size from log data stream. Got %d", workerId, conf.LogType, alloc.ID, data.Size)

		if data.Size < offsetBytes || fileNotInitiallyFound || time.Since(time.Unix(0, alloc.CreateTime)).Seconds() <= allocation.DefaultPollInterval {
			if data.Size < offsetBytes && !fileNotInitiallyFound {
				log.Warnf("[%s:%s@%s] Offset greater than total available data, got offset %d expected less than or equal to %d.", workerId, conf.LogType, alloc.ID, offsetBytes, data.Size)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_warnings", conf.Config.StatsdPrefix), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_offset_errors", conf.Config.StatsdPrefix), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_offset_errors_for_type_%s", conf.Config.StatsdPrefix, conf.LogType), 1)
			}
			offsetBytes = int64(0)
		}

		log.Infof("[%s:%s@%s] Streaming logs for path %s from offset: %d", workerId, conf.LogType, alloc.ID, conf.LogFile.Path, offsetBytes)

		stream, errors = conf.AllocationClient.StreamFile(alloc, conf.LogFile.Path, offsetBytes, streamStop)

		if len(conf.LogFile.Type) == 0 {
			log.Errorf("[%s:%s@%s] Log file type must be set.", workerId, conf.LogType, alloc.ID)
			triggerCancel(conf.CancelChannels)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
			return
		}

		itemType = conf.LogFile.Type

		if len(conf.LogFile.Delim) > 0 {
			delim = conf.LogFile.Delim
		}
	case "stderr", "stdout":
		size := conf.AllocationClient.GetLogSize(conf.LogType, alloc, conf.TaskName, 0)
		log.Infof("[%s:%s@%s] Calculated size from log data stream. Got %d", workerId, conf.LogType, alloc.ID, size)

		if size < offsetBytes || time.Since(time.Unix(0, alloc.CreateTime)).Seconds() <= allocation.DefaultPollInterval {
			if size < offsetBytes && time.Since(time.Unix(0, alloc.CreateTime)).Seconds() > allocation.DefaultPollInterval {
				log.Warnf("[%s:%s@%s] Offset greater than total available data, got offset \"%d\" expected less than or equal to \"%d\"", workerId, conf.LogType, alloc.ID, offsetBytes, size)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_warnings", conf.Config.StatsdPrefix), 1)
			}
			offsetBytes = int64(0)
		}

		log.Debugf("[%s:%s@%s] Streaming logs from offset: %d", workerId, conf.LogType, alloc.ID, offsetBytes)

		stream, errors = conf.AllocationClient.StreamLog(conf.LogType, alloc, conf.TaskName, offsetBytes, streamStop)

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
		log.Errorf("[%s:%s@%s] Invalid log type provided.", workerId, conf.LogType, alloc.ID)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
		incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
		return
	}

StreamLoop:
	for {
		select {
		case err := <-errors:
			if strings.Contains(err.Error(), "no such file or directory") {
				log.Warningf("[%s:%s@%s] Unable to find file: %s", workerId, conf.LogType, alloc.ID, err)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_warnings", conf.Config.StatsdPrefix), 1)
			} else {
				alloc, _ := conf.AllocationClient.GetAllocationInfo(conf.Allocation.ID)

				if alloc != nil {
					log.Errorf("[%s:%s@%s] Error while streaming: %s, Alloc State: %s", workerId, conf.LogType, alloc.ID, err, alloc.ClientStatus)
				} else {
					log.Errorf("[%s:%s@%s] Error while streaming: %s", workerId, conf.LogType, alloc.ID, err)
				}

				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_streaming_errors", conf.Config.StatsdPrefix), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_streaming_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_streaming_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
			}

			triggerCancel(conf.CancelChannels)

			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))

			break StreamLoop
		case <-conf.CancelChannel:
			alloc, _ := conf.AllocationClient.GetAllocationInfo(conf.Allocation.ID)

			expectedRestart := false

			if alloc != nil && (alloc.ClientStatus == "running" || alloc.ClientStatus == "restarting") {
				expectedRestart = true
			}

			log.Warnf(
				"[%s:%s@%s] Received cancel for Task: %s, Expected Restart: %t",
				workerId,
				conf.LogType,
				alloc.ID,
				conf.DisplayName,
				expectedRestart,
			)

			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_warnings", conf.Config.StatsdPrefix), 1)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_received", conf.Config.StatsdPrefix), 1)
			break StreamLoop
		case data, ok := <-stream:
			if !ok {
				log.Errorf(
					"[%s:%s@%s] Not ok when reading from stream: Task: %s",
					workerId,
					conf.LogType,
					alloc.ID,
					conf.DisplayName,
				)

				triggerCancel(conf.CancelChannels)

				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)

				break StreamLoop
			}

			var bytes int

			if len(data.FileEvent) > 0 {
				log.Warnf(
					"[%s:%s@%s] Resetting offset due to file event: %s Task: %s",
					workerId,
					conf.LogType,
					alloc.ID,
					data.FileEvent,
					conf.DisplayName,
				)

				triggerCancel(conf.CancelChannels)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_warnings", conf.Config.StatsdPrefix), 1)

				break StreamLoop
			} else {
				bytes = len(data.Data)

				if alloc.ID != conf.Config.SelfAlloc && len(conf.Config.SelfAlloc) > 0 {
					log.Debugf("[%s:%s@%s] Processing %d bytes", workerId, conf.LogType, alloc.ID, bytes)
				}

				if offsetBytes > int64(0) && pair != nil {
					value := string(data.Data)

					logItems := []logItem{{Message: ""}}

					reg, err := regexp.Compile(delim)

					if err != nil {
						log.Errorf("[%s:%s@%s] Error compiling regex: %s", workerId, conf.LogType, alloc.ID, err)

						triggerCancel(conf.CancelChannels)
						incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
						incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
						incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
						incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
						break StreamLoop
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
						log.Debugf("[%s:%s@%s] Found log items: %d", workerId, conf.LogType, alloc.ID, len(logItems))
					}

					sentBytes := int64(0)
					sentMessages := int64(0)

					for _, item := range logItems {
						item.Type = itemType
						item.Allocation = alloc.ID
						item.Job = alloc.JobID
						item.Group = alloc.TaskGroup
						item.Task = conf.TaskName

						msg, err := json.Marshal(item)

						if err != nil {
							log.Errorf("[%s:%s@%s] %s", workerId, conf.LogType, alloc.ID, err)

							triggerCancel(conf.CancelChannels)
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
							break StreamLoop
						}

						if alloc.ID != conf.Config.SelfAlloc && len(conf.Config.SelfAlloc) > 0 {
							log.Debugf("[%s:%s@%s] Sending message with bytes: %d", workerId, conf.LogType, alloc.ID, len(item.Message))
						}

						if conf.SendLogs {
							err = conf.Logzio.Send(msg)

							if err != nil {
								log.Errorf("[%s:%s@%s] Error sending to logz.io: %s", workerId, conf.LogType, alloc.ID, err)
								incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
								incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
								incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
							} else {
								sentBytes = sentBytes + int64(len(item.Message))
								sentMessages = sentMessages + 1
								incrementMetric(metrics, fmt.Sprintf("%slogshipper_logs_shipped", conf.Config.StatsdPrefix), 1)
								incrementMetric(metrics, fmt.Sprintf("%slogshipper_logs_shipped_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
								incrementMetric(metrics, fmt.Sprintf("%slogshipper_logs_shipped_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
								incrementMetric(metrics, fmt.Sprintf("%slogshipper_logs_shipped_for_type_%s", conf.Config.StatsdPrefix, conf.LogType), 1)
								incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_sent", conf.Config.StatsdPrefix), len(item.Message))
								incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_sent_for_type_%s", conf.Config.StatsdPrefix, conf.LogType), len(item.Message))
								incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_sent_worker_%s", conf.Config.StatsdPrefix, workerId), len(item.Message))
								incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_sent_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), len(item.Message))
							}
						} else {
							sentBytes = sentBytes + int64(len(item.Message))
							sentMessages = sentMessages + 1
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_logs_shipped", conf.Config.StatsdPrefix), 1)
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_logs_shipped_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_logs_shipped_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_logs_shipped_for_type_%s", conf.Config.StatsdPrefix, conf.LogType), 1)
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_sent", conf.Config.StatsdPrefix), len(item.Message))
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_sent_for_type_%s", conf.Config.StatsdPrefix, conf.LogType), len(item.Message))
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_sent_worker_%s", conf.Config.StatsdPrefix, workerId), len(item.Message))
							incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_sent_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), len(item.Message))
						}
					}

					if alloc.ID != conf.Config.SelfAlloc && len(conf.Config.SelfAlloc) > 0 {
						log.Debugf("[%s:%s@%s] Sent %d/%d messages with %d/%d bytes", workerId, conf.LogType, alloc.ID, sentMessages, len(logItems), sentBytes, bytes)
					}
				}
			}

			log.Debugf("[%s:%s@%s] Processed bytes: %d", workerId, conf.LogType, alloc.ID, bytes)

			offsetBytes = offsetBytes + int64(bytes)
			bytesRead = bytesRead + int64(bytes)

			incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_processed", conf.Config.StatsdPrefix), bytes)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_processed_for_type_%s", conf.Config.StatsdPrefix, conf.LogType), bytes)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_processed_worker_%s", conf.Config.StatsdPrefix, workerId), bytes)
			incrementMetric(metrics, fmt.Sprintf("%slogshipper_bytes_processed_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), bytes)

			if offsetBytes > bytesRead {
				log.Warnf("[%s:%s@%s] Detected offset greater than total bytes read, offset %d, bytes read %d.", workerId, conf.LogType, alloc.ID, offsetBytes, bytesRead)
				offsetBytes = bytesRead
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_warnings", conf.Config.StatsdPrefix), 1)
			}

			stats := processStats{
				BytesRead:   bytesRead,
				OffsetBytes: offsetBytes,
				LastSeen:    time.Now(),
			}

			statsJSON, err := json.Marshal(stats)

			if err != nil {
				triggerCancel(conf.CancelChannels)
				log.Errorf("[%s:%s@%s] %s", workerId, conf.LogType, alloc.ID, err)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
				break StreamLoop
			}

			p := &consul.KVPair{Key: fmt.Sprintf("%s/%s", *conf.ConsulPath, consulStatsKey), Value: []byte(statsJSON)}

			_, err = conf.KVStore.Put(p, nil)

			if err != nil {
				triggerCancel(conf.CancelChannels)
				log.Errorf("[%s:%s@%s] Error saving log shipping stats to consul: %s", workerId, conf.LogType, alloc.ID, err)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_cancellations_sent", conf.Config.StatsdPrefix), len(*conf.CancelChannels))
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors", conf.Config.StatsdPrefix), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_worker_%s", conf.Config.StatsdPrefix, workerId), 1)
				incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_errors_for_alloc_%s", conf.Config.StatsdPrefix, alloc.ID), 1)
				break StreamLoop
			}

			pair = p
		}
	}

	log.Warnf("[%s:%s@%s] Loop finished for Task: %s, Type: %s", workerId, conf.LogType, alloc.ID, conf.DisplayName, conf.LogType)
	incrementMetric(metrics, fmt.Sprintf("%slogshipper_worker_warnings", conf.Config.StatsdPrefix), 1)
	incrementMetric(metrics, fmt.Sprintf("%slogshipper_workers_finished", conf.Config.StatsdPrefix), 1)
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

func triggerCancel(channels *[]chan bool) {
	for _, c := range *channels {
		select {
		case c <- true:
		default:
		}
	}
}

func filterAllocationsExclude(vs []nomad.Allocation, id string) []nomad.Allocation {
	vsf := make([]nomad.Allocation, 0)

	for _, v := range vs {
		if v.ID != id {
			vsf = append(vsf, v)
		}
	}

	return vsf
}

func incrementMetric(channel chan<- Metric, name string, value int) {
	channel <- Metric{
		Name: name,
		Value: value,
	}
}

func metricListener(channel <-chan Metric, handlers []func(Metric)) {
	for {
		metric := <-channel

		for _, handler := range handlers {
			handler(metric)
		}
	}
}
