package bus

import (
	"context"
	"errors"
	"fmt"

	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Kafka struct {
	cl  *kgo.Client
	cfg config.KafkaConfig
}

func NewKafka(cfg config.KafkaConfig) (*Kafka, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(1<<20),
	)
	if cl == nil {
		return nil, err
	}
	if err := cl.Ping(context.Background()); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka ping: %w", err)
	}
	ensureTopic(cl, cfg)
	return &Kafka{
		cl:  cl,
		cfg: cfg,
	}, nil
}

func (k *Kafka) Close() {
	k.cl.Close()
}

// ensureTopic creates the topic if it is missing. Idempotent: an existing
// topic is left alone, so every process can safely call this at startup.
func ensureTopic(cl *kgo.Client, kcfg config.KafkaConfig) error {
	adm := kadm.NewClient(cl)
	cfg := map[string]*string{
		"retention.ms": kadm.StringPtr("86400000"), // 24h
	}
	resp, err := adm.CreateTopic(context.Background(), int32(kcfg.Partitions), -1, cfg, kcfg.Topic)
	if err != nil {
		return err
	}
	if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return resp.Err
	}
	return nil
}

func (k *Kafka) PublishBatch(ctx context.Context, rows []store.OutboxRow) error {
	recs := make([]*kgo.Record, len(rows))
	for i, r := range rows {
		recs[i] = &kgo.Record{
			Topic: k.cfg.Topic,
			Key:   []byte(r.Key),
			Value: r.Payload,
		}
	}
	// ProduceSync waits for all records: we must not mark them published
	// before the broker has acknowledged them.
	return k.cl.ProduceSync(ctx, recs...).FirstErr()
}
