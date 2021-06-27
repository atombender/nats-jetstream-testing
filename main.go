package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	flags "github.com/jessevdk/go-flags"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

func main() {
	var opts struct {
		ShowStats             bool    `long:"show-stats" description:"Show stat"`
		PushMode              bool    `long:"push-mode" description:"Use push"`
		PublishRate           float64 `long:"publish-rate" description:"Publish rate" default:"1"`
		NumConsumers          int     `long:"consumers" description:"Number of consumers" default:"10000"`
		NumPublishers         int     `long:"publishers" description:"Number of publishers" default:"1"`
		NumWorkersPerConsumer int     `long:"workers-per-consumer" description:"Number of workers per consumer" default:"1"`
	}

	parser := flags.NewParser(&opts, flags.HelpFlag|flags.PassDoubleDash)

	args, err := parser.Parse()
	if err != nil {
		if e, ok := err.(*flags.Error); ok && e.Type == flags.ErrHelp {
			parser.WriteHelp(os.Stdout)
			os.Exit(2)
			return
		}
		log.Fatal().Err(err).Msg("Failed to parse arguments")
	}
	if len(args) != 0 {
		log.Fatal().Msg("This command takes no arguments")
	}

	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		fatal(fmt.Errorf("NATS connect: %w", err))
	}
	defer conn.Close()

	js, err := conn.JetStream(
		nats.PublishAsyncMaxPending(256),
		nats.PublishAsyncErrHandler(func(js nats.JetStream, msg *nats.Msg, err error) {
		}),
	)
	if err != nil {
		fatal(fmt.Errorf("getting JetStream: %w", err))
	}

	if opts.ShowStats {
		go func() {
			for range time.NewTicker(1 * time.Second).C {
				for c := range js.ConsumersInfo("bigtest") {
					log.Info().Msgf("Consumer info: %q: pending=%d, ackPending=%d, waiting=%d",
						c.Name, c.NumPending, c.NumAckPending, c.NumWaiting)
				}
			}
		}()
	}

	_, err = js.StreamInfo("bigtest")
	if err != nil {
		log.Printf("Creating main stream")
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "bigtest",
			Subjects: []string{"bigtest-events.*"},
			Discard:  nats.DiscardOld,
			Replicas: 1,
		})
		if err != nil {
			fatal(fmt.Errorf("creating stream: %w", err))
		}
	}

	numQueues := 10_000

	var receiveCount int64
	var sendCount int64
	var erroringCount int64

	go func() {
		var prevCount int64
		prevTime := time.Now()
		for range time.NewTicker(5 * time.Second).C {
			now := time.Now()
			count := atomic.LoadInt64(&receiveCount)
			deltaT := now.Sub(prevTime)
			delta := float64(count - prevCount)
			log.Printf("Stats: Total rcvd: %8d  pending: %8d  erroring: %8d  consume rate: %8.1f items/sec",
				atomic.LoadInt64(&receiveCount),
				atomic.LoadInt64(&sendCount)-atomic.LoadInt64(&receiveCount),
				atomic.LoadInt64(&erroringCount),
				delta/deltaT.Seconds(),
			)
			prevTime = now
			prevCount = count
		}
	}()

	log.Printf("Adding %d consumers...", numQueues)
	for queueID := 0; queueID < numQueues; queueID++ {
		queueID := queueID

		subj := fmt.Sprintf("bigtest-events.%d", queueID)

		consumerID := fmt.Sprintf("consumer%d", queueID)

		_, err = js.ConsumerInfo("bigtest", consumerID)
		if err != nil {
			_, err = js.AddConsumer("bigtest", &nats.ConsumerConfig{
				Durable:       consumerID,
				FilterSubject: subj,
				DeliverPolicy: nats.DeliverNewPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
				ReplayPolicy:  nats.ReplayInstantPolicy,
				AckWait:       1 * time.Minute,
				// RateLimit:     0,
				// MaxWaiting:    10,
				// MaxAckPending: 1000,
				// Heartbeat:   5 * time.Second,
			})
			if err != nil {
				fatal(fmt.Errorf("creating consumer: %w", err))
			}
		}

		for workerID := 1; workerID <= opts.NumWorkersPerConsumer; workerID++ {
			workerID := workerID
			go func() {
				for {
					sub, err := js.PullSubscribe(subj, consumerID, nats.ManualAck())
					if err != nil {
						log.Printf("Error subscribing: %s", err)
						time.Sleep(1 * time.Second)
						continue
					}
					defer func() {
						_ = sub.Unsubscribe()
					}()

					erroring := false
					for {
						msgs, err := sub.Fetch(1, nats.MaxWait(60*time.Second))
						if err != nil {
							isErr := true
							switch err.Error() {
							case "nats: no messages":
								isErr = false
							case "nats: timeout":
							case "nats: Request Timeout":
							default:
								log.Printf("[Error] fetching: %s", err)
							}
							if isErr && !erroring {
								erroring = true
								atomic.AddInt64(&erroringCount, 1)
							}
							continue
						}

						if erroring {
							erroring = false
							atomic.AddInt64(&erroringCount, -1)
						}
						for _, msg := range msgs {
							atomic.AddInt64(&receiveCount, 1)
							log.Printf("[Consumer worker %d] Msg to %s: %s", workerID, msg.Subject, string(msg.Data))
							if err = msg.Ack(); err != nil {
								fatal(fmt.Errorf("acking: %w", err))
							}
						}
					}
				}
			}()
		}
	}
	log.Printf("Added all consumers")

	if opts.NumPublishers > 0 && opts.PublishRate > 0 {
		log.Printf("Starting %d publishers...", opts.NumPublishers)
		var lastMsgID int64
		for i := 1; i <= opts.NumPublishers; i++ {
			go func() {
				for {
					id := rand.Intn(numQueues)
					atomic.AddInt64(&sendCount, 1)
					msgID := atomic.AddInt64(&lastMsgID, 1)
					_, err = js.Publish(fmt.Sprintf("bigtest-events.%d", id),
						[]byte(fmt.Sprintf("hello to consumer %d [msg %d]", id, msgID)))
					if err != nil {
						fatal(fmt.Errorf("publishing: %w", err))
					}
					time.Sleep(time.Duration(1000/opts.PublishRate) * time.Millisecond)
				}
			}()
		}
	}

	select {}
}

func fatal(err error) {
	log.Fatal().Err(err).Msgf("Fatal: %s", err)
}
