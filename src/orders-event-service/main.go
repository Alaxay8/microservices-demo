package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

type kafkaRestConsumer struct {
	client          *http.Client
	restURL         string
	topic           string
	group           string
	instanceID      string
	instanceBaseURL string
}

type createConsumerResponse struct {
	InstanceID string `json:"instance_id"`
	BaseURI    string `json:"base_uri"`
}

type kafkaRecord struct {
	Key       *string         `json:"key"`
	Value     json.RawMessage `json:"value"`
	Partition int             `json:"partition"`
	Offset    int64           `json:"offset"`
}

type orderEvent struct {
	OrderID          string           `json:"order_id"`
	UserID           string           `json:"user_id"`
	Email            string           `json:"email"`
	TotalCurrency    string           `json:"total_currency"`
	TotalUnits       int64            `json:"total_units"`
	TotalNanos       int32            `json:"total_nanos"`
	ShippingTracking string           `json:"shipping_tracking_id"`
	ShippingAddress  map[string]any   `json:"shipping_address"`
	Items            []map[string]any `json:"items"`
	CreatedAt        string           `json:"created_at"`
}

var (
	log        *logrus.Logger
	pollPeriod = 5 * time.Second
)

func init() {
	log = logrus.New()
	log.Level = logrus.DebugLevel
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.Out = os.Stdout
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	restURL := mustGetEnv("KAFKA_REST_URL")
	topic := mustGetEnv("ORDER_EVENTS_TOPIC")
	group := os.Getenv("ORDER_EVENTS_CONSUMER_GROUP")
	if group == "" {
		group = "orders-event-service"
	}

	if poll := os.Getenv("ORDER_EVENTS_POLL_INTERVAL_SEC"); poll != "" {
		if sec, err := strconv.Atoi(poll); err == nil && sec > 0 {
			pollPeriod = time.Duration(sec) * time.Second
		} else {
			log.Warnf("invalid ORDER_EVENTS_POLL_INTERVAL_SEC=%q, using default %s", poll, pollPeriod)
		}
	}

	consumer := &kafkaRestConsumer{
		client:  &http.Client{Timeout: 30 * time.Second},
		restURL: strings.TrimSuffix(restURL, "/"),
		topic:   topic,
		group:   group,
	}

	if err := consumer.connect(ctx); err != nil {
		log.Fatalf("failed to connect to Kafka REST proxy: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.consume(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Errorf("event consumption stopped: %v", err)
		}
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Infof("orders-event-service listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-serverErr:
		if err != nil {
			log.Errorf("http server error: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warnf("http server shutdown error: %v", err)
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	consumer.close(closeCtx)

	wg.Wait()
	log.Info("orders-event-service exited")
}

func mustGetEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("environment variable %s must be set", key)
	}
	return val
}

func (c *kafkaRestConsumer) connect(ctx context.Context) error {
	name := fmt.Sprintf("orders-event-consumer-%d", time.Now().UnixNano())
	payload := map[string]any{
		"name":               name,
		"format":             "json",
		"auto.offset.reset":  "earliest",
		"auto.commit.enable": true,
	}

	buf, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("%s/consumers/%s", c.restURL, c.group)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.kafka.v2+json")
	req.Header.Set("Accept", "application/vnd.kafka.v2+json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create consumer failed: %s", strings.TrimSpace(string(body)))
	}

	var consumerResp createConsumerResponse
	if err := json.NewDecoder(resp.Body).Decode(&consumerResp); err != nil {
		return err
	}

	c.instanceID = consumerResp.InstanceID
	c.instanceBaseURL = strings.TrimSuffix(consumerResp.BaseURI, "/")
	log.Infof("created consumer instance %q for topic %q", c.instanceID, c.topic)

	return c.subscribe(ctx)
}

func (c *kafkaRestConsumer) subscribe(ctx context.Context) error {
	payload := map[string]any{
		"topics": []string{c.topic},
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("%s/subscription", c.instanceBaseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.kafka.v2+json")
	req.Header.Set("Accept", "application/vnd.kafka.v2+json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("subscribe failed: %s", strings.TrimSpace(string(body)))
	}
	log.Infof("subscribed consumer %q to topic %q", c.instanceID, c.topic)
	return nil
}

func (c *kafkaRestConsumer) consume(ctx context.Context) error {
	for {
		if err := c.fetchAndLog(ctx); err != nil {
			log.Warnf("failed to fetch events: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollPeriod):
		}
	}
}

func (c *kafkaRestConsumer) fetchAndLog(ctx context.Context) error {
	if c.instanceBaseURL == "" {
		return errors.New("consumer not initialized")
	}

	endpoint := fmt.Sprintf("%s/records", c.instanceBaseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.kafka.json.v2+json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("fetch records failed: %s", strings.TrimSpace(string(body)))
	}

	var records []kafkaRecord
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}

	for _, rec := range records {
		var evt orderEvent
		if err := json.Unmarshal(rec.Value, &evt); err != nil {
			log.Warnf("unable to decode order event: %v", err)
			continue
		}

		fields := logrus.Fields{
			"order_id":   evt.OrderID,
			"user_id":    evt.UserID,
			"email":      evt.Email,
			"created_at": evt.CreatedAt,
			"partition":  rec.Partition,
			"offset":     rec.Offset,
		}
		if evt.TotalCurrency != "" {
			fields["total_currency"] = evt.TotalCurrency
			fields["total_units"] = evt.TotalUnits
			fields["total_nanos"] = evt.TotalNanos
		}
		if evt.ShippingTracking != "" {
			fields["shipping_tracking_id"] = evt.ShippingTracking
		}
		log.WithFields(fields).Info("order event consumed")
	}

	return c.commit(ctx, records)
}

func (c *kafkaRestConsumer) commit(ctx context.Context, records []kafkaRecord) error {
	offsets := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		offsets = append(offsets, map[string]any{
			"topic":     c.topic,
			"partition": rec.Partition,
			"offset":    rec.Offset + 1,
		})
	}

	payload := map[string]any{"offsets": offsets}
	buf, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("%s/offsets", c.instanceBaseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.kafka.v2+json")
	req.Header.Set("Accept", "application/vnd.kafka.v2+json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("commit offsets failed: %s", strings.TrimSpace(string(body)))
	}
	return nil
}

func (c *kafkaRestConsumer) close(ctx context.Context) {
	if c.instanceBaseURL == "" {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.instanceBaseURL, nil)
	if err != nil {
		log.Warnf("failed to build consumer delete request: %v", err)
		return
	}
	resp, err := c.client.Do(req)
	if err != nil {
		log.Warnf("failed to delete consumer: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		log.Warnf("consumer delete returned status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	} else {
		log.Infof("consumer instance %q deleted", c.instanceID)
	}
}
