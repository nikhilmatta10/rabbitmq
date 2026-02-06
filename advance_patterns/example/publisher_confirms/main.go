package main

import (
    "context"
    "fmt"
    "log"
    "strconv"
    "sync"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
)

const (
    queueName   = "pub_confirms_queue"
    messageCount = 50_000
    amqpURL      = "amqp://guest:guest@localhost:5672/"
)

// createConnection is analogous to the Java createConnection()
func createConnection() (*amqp.Connection, error) {
    return amqp.Dial(amqpURL)
}

// declareQueue ensures the queue exists with the same properties as in Java:
// durable=false, exclusive=false, autoDelete=true
func declareQueue(ch *amqp.Channel) error {
    _, err := ch.QueueDeclare(
        queueName,
        false, // durable
        true,  // autoDelete
        false, // exclusive
        false, // noWait
        nil,   // args
    )
    return err
}

// publishMessagesIndividually publishes messages one by one with the option to enable publisher confirms.
// If confirms are enabled, it waits up to 5 seconds for each message confirmation (like waitForConfirmsOrDie(5000)).
func publishMessagesIndividually(isPubConfEnabled bool) error {
    conn, err := createConnection()
    if err != nil {
        return err
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        return err
    }
    defer ch.Close()

    if err := declareQueue(ch); err != nil {
        return err
    }

    if isPubConfEnabled {
        // enable confirms
        if err := ch.Confirm(false); err != nil {
            return fmt.Errorf("failed to enable confirms: %w", err)
        }
    }

    start := time.Now()

    for i := 0; i <= messageCount; i++ {
        body := strconv.Itoa(i)

        // If confirms enabled, use deferred confirm API to wait per message
        if isPubConfEnabled {
            dc, err := ch.PublishWithDeferredConfirm(
                "",        // exchange
                queueName, // routing key
                false,     // mandatory
                false,     // immediate
                amqp.Publishing{
                    ContentType: "text/plain",
                    Body:        []byte(body),
                },
            )
            if err != nil {
                return fmt.Errorf("publish failed: %w", err)
            }

            // 5 second timeout (like waitForConfirmsOrDie(5_000))
            ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            ok,_ := dc.WaitContext(ctx)
            cancel()
            if !ok {
                return fmt.Errorf("message confirm timeout or nack for body=%s", body)
            }
        } else {
            // No confirms: fire-and-forget
            if err := ch.Publish(
                "",
                queueName,
                false,
                false,
                amqp.Publishing{
                    ContentType: "text/plain",
                    Body:        []byte(body),
                },
            ); err != nil {
                return fmt.Errorf("publish failed: %w", err)
            }
        }
    }

    elapsed := time.Since(start)
    fmt.Printf("Published %d, messages (isPubConfEnabled=%v) in %d, ms\n",
        messageCount, isPubConfEnabled, elapsed.Milliseconds())

    return nil
}

// publishMessagesInBatch publishes messages and confirms them in batches.
// Batch size is 100, with a 5-second per-message wait in each batch (like Java).
func publishMessagesInBatch() error {
    conn, err := createConnection()
    if err != nil {
        return err
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        return err
    }
    defer ch.Close()

    if err := declareQueue(ch); err != nil {
        return err
    }

    if err := ch.Confirm(false); err != nil {
        return fmt.Errorf("failed to enable confirms: %w", err)
    }

    const batchSize = 100
    var pend []*amqp.DeferredConfirmation
    pend = pend[:0]

    start := time.Now()

    for i := 0; i < messageCount; i++ {
        body := strconv.Itoa(i)
        dc, err := ch.PublishWithDeferredConfirm(
            "",
            queueName,
            false,
            false,
            amqp.Publishing{
                ContentType: "text/plain",
                Body:        []byte(body),
            },
        )
        if err != nil {
            return fmt.Errorf("publish failed: %w", err)
        }
        pend = append(pend, dc)

        if len(pend) == batchSize {
            // Wait for the batch
            for _, d := range pend {
                ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
                ok, _ := d.WaitContext(ctx)
                cancel()
                if !ok {
                    return fmt.Errorf("batch confirm contained timeout/nack")
                }
            }
            pend = pend[:0]
        }
    }

    // Flush any remainder
    if len(pend) > 0 {
        for _, d := range pend {
            ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            ok, _ := d.WaitContext(ctx)
            cancel()
            if !ok {
                return fmt.Errorf("final batch confirm contained timeout/nack")
            }
        }
    }

    elapsed := time.Since(start)
    fmt.Printf("Published %d, messages in batch in %d, ms\n", messageCount, elapsed.Milliseconds())
    return nil
}

// waitUntil polls a condition until it's true or timeout expires (like the Java waitUntil()).
func waitUntil(timeout time.Duration, condition func() bool) bool {
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if condition() {
            return true
        }
        time.Sleep(100 * time.Millisecond)
    }
    return condition()
}

// handlePublishConfirmsAsynchronously publishes all messages and processes ACK/NACKs
// via NotifyPublish() while tracking outstanding publishes in a concurrent map.
func handlePublishConfirmsAsynchronously() error {
    conn, err := createConnection()
    if err != nil {
        return err
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        return err
    }
    defer ch.Close()

    if err := declareQueue(ch); err != nil {
        return err
    }

    // Enable confirms and register listener
    if err := ch.Confirm(false); err != nil {
        return fmt.Errorf("failed to enable confirms: %w", err)
    }

    // Thread-safe structure for outstanding confirms: seqNo -> body
    var (
        mu        sync.Mutex
        outstanding = make(map[uint64]string)
    )

    // Confirm notifications channel
    confirms := ch.NotifyPublish(make(chan amqp.Confirmation, messageCount))

    // Goroutine that consumes ACK/NACK and updates map
    go func() {
        for c := range confirms {
            mu.Lock()
            body := outstanding[c.DeliveryTag]
            delete(outstanding, c.DeliveryTag)
            mu.Unlock()

            if !c.Ack {
                // In the Java example, the handler logs body and multiple flag.
                // amqp091-go Confirmation does not expose "multiple", so we log per-message.
                log.Printf("NACK for seq=%d body=%s\n", c.DeliveryTag, body)
            }
        }
    }()

    start := time.Now()

    // Publish loop
    for i := 0; i < messageCount; i++ {
        body := strconv.Itoa(i)

        // Retrieve next sequence number before publishing
        seq := ch.GetNextPublishSeqNo()

        mu.Lock()
        outstanding[seq] = body
        mu.Unlock()

        if err := ch.Publish(
            "",
            queueName,
            false,
            false,
            amqp.Publishing{
                ContentType: "text/plain",
                Body:        []byte(body),
            },
        ); err != nil {
            return fmt.Errorf("publish failed for seq=%d body=%s: %w", seq, body, err)
        }
    }

    // Wait until everything is confirmed (ACK or NACK), up to 60 seconds
    ok := waitUntil(60*time.Second, func() bool {
        mu.Lock()
        defer mu.Unlock()
        return len(outstanding) == 0
    })
    if !ok {
        return fmt.Errorf("all messages could not be confirmed in 60 seconds")
    }

    elapsed := time.Since(start)
    fmt.Printf("Published %d, messages and handled confirms asynchronously in %d, ms\n",
        messageCount, elapsed.Milliseconds())

    return nil
}

func main() {
    if err := publishMessagesIndividually(false); err != nil {
        log.Fatalf("publishMessagesIndividually(false) error: %v", err)
    }
    if err := publishMessagesIndividually(true); err != nil {
        log.Fatalf("publishMessagesIndividually(true) error: %v", err)
    }
    if err := publishMessagesInBatch(); err != nil {
        log.Fatalf("publishMessagesInBatch() error: %v", err)
    }
    if err := handlePublishConfirmsAsynchronously(); err != nil {
        log.Fatalf("handlePublishConfirmsAsynchronously() error: %v", err)
    }

    fmt.Println("Done")
}