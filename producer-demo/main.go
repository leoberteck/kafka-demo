package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/tools/tls"
)

var (
	brokerList    = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster. You can also set the KAFKA_PEERS environment variable")
	topic         = flag.String("topic", "", "REQUIRED: the topic to produce to")
	partitioner   = flag.String("partitioner", "", "The partitioning scheme to use. Can be `hash`, `manual`, or `random`")
	partition     = flag.Int("partition", -1, "The partition to produce to.")
	verbose       = flag.Bool("verbose", false, "Turn on sarama logging to stderr")
	showMetrics   = flag.Bool("metrics", false, "Output metrics on successful publish to stderr")
	silent        = flag.Bool("silent", false, "Turn off printing the message's topic, partition, and offset to stdout")
	tlsEnabled    = flag.Bool("tls-enabled", false, "Whether to enable TLS")
	tlsSkipVerify = flag.Bool("tls-skip-verify", false, "Whether skip TLS server cert verification")
	tlsClientCert = flag.String("tls-client-cert", "", "Client cert for client authentication (use with -tls-enabled and -tls-client-key)")
	tlsClientKey  = flag.String("tls-client-key", "", "Client key for client authentication (use with tls-enabled and -tls-client-cert)")
	x1            = flag.Float64("x1", -90, "bbox x lower bound")
	y1            = flag.Float64("y1", -90, "bbox y lower bound")
	x2            = flag.Float64("x2", 90, "bbox x upper bound")
	y2            = flag.Float64("y2", 90, "bbox y upper bound")
	millis        = flag.Int("delay", 1000, "delay between messages in milliseconds")
	workers       = flag.Int("workers", 1, "number of parallel workers")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	/*Cria contexto que responde ao estimulo do Ctrl+C no terminal*/
	ctx := context.Background()

	// trap Ctrl+C and call cancel on the context
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

	/*Validação de parâmetros de entrada*/

	if *brokerList == "" {
		printUsageErrorAndExit("no -brokers specified. Alternatively, set the KAFKA_PEERS environment variable")
	}

	if *topic == "" {
		printUsageErrorAndExit("no -topic specified")
	}

	if *verbose {
		sarama.Logger = logger
	}

	if *partitioner == "manual" && *partition == -1 {
		printUsageErrorAndExit("-partition is required when partitioning manually")
	}

	addrs := strings.Split(*brokerList, ",")

	/*Criação do objeto de configuração */

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	switch *partitioner {
	case "":
		if *partition >= 0 {
			config.Producer.Partitioner = sarama.NewManualPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewHashPartitioner
		}
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if *partition == -1 {
			printUsageErrorAndExit("-partition is required when partitioning manually")
		}
	default:
		printUsageErrorAndExit(fmt.Sprintf("Partitioner %s not supported.", *partitioner))
	}

	if *tlsEnabled {
		tlsConfig, err := tls.NewConfig(*tlsClientCert, *tlsClientKey)
		if err != nil {
			printErrorAndExit(69, "Failed to create TLS config: %s", err)
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Config.InsecureSkipVerify = *tlsSkipVerify
	}

	client, err := sarama.NewClient(addrs, config)

	if err != nil {
		printErrorAndExit(69, "Failed to open Kafka client: %s", err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			logger.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

	duration := time.Duration(*millis) * time.Millisecond
	var wg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go createWorker(ctx, &wg, client, strconv.Itoa(i), duration)
	}
	/*Create workers*/

	wg.Wait()
	fmt.Println("FINISHED")
}

func createWorker(ctx context.Context, wg *sync.WaitGroup, client sarama.Client, workerKey string, delay time.Duration) {
	defer wg.Done()
	producer, err := sarama.NewSyncProducerFromClient(client)

	if err == nil {
		defer producer.Close()
		for {
			select {
			case <-ctx.Done():
				{
					fmt.Printf("worker %s canceled", workerKey)
					break
				}
			default:
				{
					message := &sarama.ProducerMessage{Topic: *topic, Partition: int32(*partition)}
					message.Key = sarama.StringEncoder(workerKey)
					message.Value = sarama.StringEncoder(generateRandomPoint())
					partition, offset, err := producer.SendMessage(message)
					if err != nil {
						printError("Failed to produce message: %s", err)
						break
					} else if !*silent {
						fmt.Printf("worker=%s\ttopic=%s\tpartition=%d\toffset=%d\n", workerKey, *topic, partition, offset)
					}
					time.Sleep(delay)
				}
			}
		}
	} else {
		printError("Failed to open Kafka producer: %s", err)
	}
}

func generateRandomPoint() string {
	x := *x1 + rand.Float64()*(*x2-*x1)
	y := *y1 + rand.Float64()*(*y2-*y1)
	return fmt.Sprintf("[ %.6f, %.6f ]", x, y)
}

func printError(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	printError(format, values...)
	os.Exit(code)
}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func stdinAvailable() bool {
	stat, _ := os.Stdin.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}
