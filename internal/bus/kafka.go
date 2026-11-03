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

// topicFor maps a runner to its Kafka topic. All runners share one topic for
// now; per-runner topics only matter when two workloads run at the same time.
func (k *Kafka) topicFor(_ string) string {
	return k.cfg.Topic
}

func (k *Kafka) PublishBatch(ctx context.Context, rows []store.OutboxRow) error {
	recs := make([]*kgo.Record, len(rows))
	for i, r := range rows {
		recs[i] = &kgo.Record{
			Topic: k.topicFor(r.Runner),
			Key:   []byte(r.Key),
			Value: r.Payload,
		}
	}
	// ProduceSync waits for all records: we must not mark them published
	// before the broker has acknowledged them.
	return k.cl.ProduceSync(ctx, recs...).FirstErr()
}

// ConsumerLag reports the worker group's lag per partition. A group that does
// not exist yet is not an error, it just has no lag to report; and with no
// members only the partitions it has committed on are known.
func (k *Kafka) ConsumerLag(ctx context.Context) (map[int32]int64, error) {
	lags, err := kadm.NewClient(k.cl).Lag(ctx, k.cfg.Group)
	if err != nil {
		return nil, fmt.Errorf("lag: %w", err)
	}
	group := lags[k.cfg.Group]
	if err := group.Error(); err != nil {
		if errors.Is(err, kerr.GroupIDNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("lag of group %s: %w", k.cfg.Group, err)
	}

	out := make(map[int32]int64, len(group.Lag[k.cfg.Topic]))
	for part, l := range group.Lag[k.cfg.Topic] {
		if l.Err != nil {
			return nil, fmt.Errorf("lag of partition %d: %w", part, l.Err)
		}
		out[part] = l.Lag
	}
	return out, nil
}
