package main

import (
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/tools/tls"
	"github.com/gorilla/websocket"
)

var (
	brokerList    = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topic         = flag.String("topic", "", "REQUIRED: the topic to consume")
	partitions    = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	offset        = flag.String("offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
	verbose       = flag.Bool("verbose", false, "Whether to turn on sarama logging")
	tlsEnabled    = flag.Bool("tls-enabled", false, "Whether to enable TLS")
	tlsSkipVerify = flag.Bool("tls-skip-verify", false, "Whether skip TLS server cert verification")
	tlsClientCert = flag.String("tls-client-cert", "", "Client cert for client authentication (use with -tls-enabled and -tls-client-key)")
	tlsClientKey  = flag.String("tls-client-key", "", "Client key for client authentication (use with tls-enabled and -tls-client-cert)")
	addr          = flag.String("addr", "localhost:8080", "http service address")

	bufferSize = flag.Int("buffer-size", 256, "The buffer size of the message channel.")

	logger = log.New(os.Stderr, "", log.LstdFlags)

	messages chan *sarama.ConsumerMessage
	closing  chan struct{}
	wg       sync.WaitGroup
)

func init() {

}

func main() {
	flag.Parse()
	go setupConsumer()

	http.HandleFunc("/ws-teste", wsTeste)
	http.HandleFunc("/", home)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func home(w http.ResponseWriter, r *http.Request) {
	homeTemplate.Execute(w, "ws://"+r.Host+"/ws-teste")
}

func wsTeste(w http.ResponseWriter, r *http.Request) {

	c, err := websocket.Upgrade(w, r, nil, 1024, 1024)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer c.Close()
	for msg := range messages {
		fmt.Printf("Partition:\t%d\n", msg.Partition)
		fmt.Printf("Offset:\t%d\n", msg.Offset)
		fmt.Printf("Key:\t%s\n", string(msg.Key))
		fmt.Printf("Value:\t%s\n", string(msg.Value))
		fmt.Println()
		err = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("{\"key\": \"%s\", \"value\": %s}", msg.Key, msg.Value)))
		if err != nil {
			log.Println("write:", err)
			break
		}
	}
}

func setupConsumer() {
	if *brokerList == "" {
		printUsageErrorAndExit("You have to provide -brokers as a comma-separated list, or set the KAFKA_PEERS environment variable.")
	}

	if *topic == "" {
		printUsageErrorAndExit("-topic is required")
	}

	if *verbose {
		sarama.Logger = logger
	}

	var initialOffset int64
	switch *offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	config := sarama.NewConfig()
	if *tlsEnabled {
		tlsConfig, err := tls.NewConfig(*tlsClientCert, *tlsClientKey)
		if err != nil {
			printErrorAndExit(69, "Failed to create TLS config: %s", err)
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Config.InsecureSkipVerify = *tlsSkipVerify
	}

	c, err := sarama.NewConsumer(strings.Split(*brokerList, ","), config)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	partitionList, err := getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	messages = make(chan *sarama.ConsumerMessage, *bufferSize)
	closing = make(chan struct{})

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(*topic, partition, initialOffset)
		if err != nil {
			printErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	wg.Wait()
	logger.Println("Done consuming topic", *topic)
	close(messages)

	if err := c.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	if *partitions == "all" {
		return c.Partitions(*topic)
	}

	tmp := strings.Split(*partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

var homeTemplate = template.Must(template.New("").Parse(`
<!DOCTYPE html>

<html>
    <head>
        <title>
            Kafka Demo
		</title>
		<meta name='viewport' content='initial-scale=1,maximum-scale=1,user-scalable=no' />
		<script src='https://api.tiles.mapbox.com/mapbox-gl-js/v1.3.1/mapbox-gl.js'></script>
		<link href='https://api.tiles.mapbox.com/mapbox-gl-js/v1.3.1/mapbox-gl.css' rel='stylesheet' />
		<style>
		body { margin:0; padding:0; }
		#map { position:absolute; top:0; bottom:0; width:100%; }
		</style>
    </head>
	<body>
	
		<div id='map'></div>
        <script>
            window.addEventListener("load", function(evt) {

				mapboxgl.accessToken = 'pk.eyJ1IjoibGVvYmVydGVjayIsImEiOiJjamVweHZvazIwMDdmMzNwbmV5amI2bWtlIn0.LZcgYG3OwkroU6A-RNWp8g';
				var map = new mapboxgl.Map({
					container: 'map', // container id
					style: 'mapbox://styles/mapbox/streets-v11', // stylesheet location
					center: [-50.339668, -21.291778], // starting position [lng, lat]
					zoom: 12 // starting zoom
				});

				var geojson = {
					type: 'FeatureCollection',
					features: [
						
					]
				};

				map.on("load", () => {
					map.addSource("kafka_teste", {
						"type": "geojson",
						"data": geojson
					})

					map.addLayer({
						"id": "kafka_layer",
						"type": "circle",
						"source": "kafka_teste",
						'paint': {
							'circle-radius': {
							'base': 3,
							'stops': [[12, 8], [22, 180]]
							},
							'circle-color': ['get', 'color']
						}
					})
				})					

                var ws;
                var print = function(message) {
					var obj = JSON.parse(message)
					var feat = geojson.features.find(t => t.properties.id == obj.key)
					if(feat){
						feat.geometry.coordinates = obj.value
					} else {
						geojson.features.push({
							type: 'Feature',
							geometry: {
								type: 'Point',
								coordinates: obj.value
							},
							properties: {
								id: obj.key,
								color: '#'+(Math.random()*0xFFFFFF<<0).toString(16)
							}
						})
					}
					map.getSource("kafka_teste").setData(geojson)
                };
                ws = new WebSocket("{{.}}");
                ws.onopen = function(evt) {
                    console.log("OPEN");
                }
                ws.onclose = function(evt) {
                    console.log("CLOSE");
                    ws = null;
                }
                ws.onerror = function(evt) {
                    console.log("ERROR: " + evt.data);
				}
				ws.onmessage = function(evt) {
                    print(evt.data);
                }
            });
        </script>
    </body>
</html>

`))
