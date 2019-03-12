package setup

import (
	"flag"
	"log"
	"os"
	"strconv"
)

type Config struct {
	NomadAddr     string
	NomadClientID string
	ConsulAddr    string
	ConsulPath    string
	LogzIOAddr    string
	LogzIOToken   string
	QueueDir      string

	Verbose bool
	Debug   bool
	NoSend  bool
	Profile bool
	LogzioDebug bool

	MaxAge int

	SelfAlloc string

	StatsdHost string
	StatsdPort int
	StatsdPrefix string
}

func NewConfig() (*Config, error) {
	args := os.Args[1:]

	config := Config{}

	flags := flag.NewFlagSet("command", flag.ContinueOnError)
	flags.StringVar(&config.NomadAddr, "nomad", "http://127.0.0.1:4646", "The allocation address to talk to.")
	flags.StringVar(&config.NomadClientID, "node", "", "The ID of the allocation client/node to scrape logs from.")
	flags.StringVar(&config.ConsulAddr, "consul", "127.0.0.1:8500", "The consul address to talk to.")
	flags.StringVar(&config.LogzIOToken, "logz-token", "", "Your logz.io token.")
	flags.StringVar(&config.LogzIOAddr, "logz-addr", "https://listener-eu.logz.io:8071", "The logz.io endpoint.")
	flags.BoolVar(&config.Verbose, "verbose", false, "Enable verbose logging.")
	flags.BoolVar(&config.Debug, "debug", false, "Enable debug mode.")
	flags.BoolVar(&config.LogzioDebug, "logzio-debug", false, "Enable debug mode for the logzio sender.")
	flags.BoolVar(&config.NoSend, "no-send", false, "Do not ship any logs, dry run.")
	flags.BoolVar(&config.Profile, "profile", false, "Profile the cpu usage.")
	flags.IntVar(&config.MaxAge, "max-age", 7, "Set the maximum age in days for allocation log state to be stored in consul for.")
	flags.StringVar(&config.ConsulPath, "consul-path", "logzio-nomad", "The KV path in consul to store allocation log state.")
	flags.StringVar(&config.QueueDir, "queue-dir", ".Queue", "The directory to store logzio messages before sending.")
	flags.StringVar(&config.SelfAlloc, "self-alloc", "", "The alloc id for the current job in nomad.")
	flags.StringVar(&config.StatsdHost, "statsd-host", "", "The host to connect to for statsd metrics.")
	flags.IntVar(&config.StatsdPort, "statsd-port", 0, "The port to connect to for statsd metrics.")
	flags.StringVar(&config.StatsdPrefix, "statsd-prefix", "", "Prefix metrics with the given key.")

	err := flags.Parse(args)

	if err != nil {
		return nil, err
	}

	args = flags.Args()

	if envToken := os.Getenv("LOGZIO_TOKEN"); config.LogzIOToken == "" && envToken != "" {
		config.LogzIOToken = envToken
	} else if config.LogzIOToken == "" {
		log.Fatal("No logzio token was provided as an argument or in the LOGZIO_TOKEN env variable.")
	}

	if envNomadClientID := os.Getenv("NOMAD_CLIENT_ID"); config.NomadClientID == "" && envNomadClientID != "" {
		config.NomadClientID = envNomadClientID
	} else if config.NomadClientID == "" {
		log.Fatal("No nomad client/node id was provided as an argument or in the NOMAD_CLIENT_ID env variable.")
	}

	if envStatsdHost := os.Getenv("STATSD_HOST"); config.StatsdHost == "" && envStatsdHost != "" {
		config.StatsdHost = envStatsdHost
	}

	if envStatsdPort := os.Getenv("STATSD_PORT"); config.StatsdPort == 0 && envStatsdPort != "" {
		port, _ := strconv.Atoi(envStatsdPort)
		config.StatsdPort = port
	}

	return &config, nil
}
