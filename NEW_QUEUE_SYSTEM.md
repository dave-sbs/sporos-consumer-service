# PROBLEM STATEMENT:
## Reliability
1. When a new alert is created, sometimes the server responds with some sort of error and the alert doesn’t get matched to bills. When I added retries, there wasn’t enough timeouts in place and it got stuck in a retry loop
2. When we do the bulk matching on new uploads, a large portion of the requests also fail. The current retry system is not necessarily the best for the problem we’re facing
3. While you could argue that the server was overwhelmed in the case of the bulk requests in ‘case 2’, I don’t understand why it produces errors for ‘case 1’. Need to investigate further
Proposal: I think instead of exposing the server endpoint directly we need some sort of rate limiting layer to properly dictate the flow of requests, responses, and handle retries.
The server I'm speaking of is a Langsmith server hosted on the Langgraph Cloud Platform, running AI agents and workflows, with tracing and all that stuff. The edge function gets triggered and then sends a request to the server. I'll include that as well as another trigger that invokes the second function which essentially takes in all existing trackers and batches them, then does concurrent processing per batch if I'm not mistaken. I've also included that code.

## Fundmental Issues Identified:
* AI workloads have
   * variable latency
   * memory pressure (from LLMs)
   * resource contention
* Due to the AI Inference that Langsmith has to handle, basic retries would not necessarily solve the problem.

## High Level Suggestions: Client Side Architectural Changes
* Adaptive Concurrency Controls
   * Circuit Breakers
   * Adaptive Rate Limiting
* Intelligent Queuing
* AI-specific optimizations

## Server Setup and Costs
Development Server:
* 1 CPU, 1 GB RAM.
* Up to 1 replica.
* 10 GB disk no backups
Production Server:
* 2 **CPU, 2 GB RAM
* Up to 10 replicas
* Autoscaling disk, automatic backups, highly available

# Recommended Architecture

Given your LangGraph constraints and requirements, I recommend a **PostgreSQL-backed queue with Redis caching** for optimal balance of reliability, cost, and simplicity.

### Why This Approach?

1. **Dev Server Constraints**: With only 1 CPU/1GB RAM, you need client-side flow control
2. **ACID Guarantees**: PostgreSQL ensures no message loss during retries/failures
3. **Built-in Priority**: PostgreSQL can handle your priority requirements natively
4. **Cost-Effective**: Uses Supabase's existing PostgreSQL (no additional infrastructure)

## Detailed Implementation Plan

### 1. Queue Schema Design

```sql
-- Main queue table with priority support
CREATE TABLE alert_queue (
    id BIGSERIAL PRIMARY KEY,
    trigger_type VARCHAR(20) NOT NULL, -- 'new_alert' or 'bulk_upload'
    alert_id UUID NOT NULL,
    payload JSONB NOT NULL,
    priority INTEGER DEFAULT 5, -- 1=highest, 10=lowest
    status VARCHAR(20) DEFAULT 'pending',
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    created_at TIMESTAMP DEFAULT NOW(),
    scheduled_for TIMESTAMP DEFAULT NOW(), -- For delayed retries
    locked_at TIMESTAMP,
    locked_by VARCHAR(100),
    completed_at TIMESTAMP,
    error_message TEXT,
    
    -- Indexes for performance
    INDEX idx_queue_processing (status, priority, scheduled_for),
    INDEX idx_alert_id (alert_id),
    INDEX idx_locked_by (locked_by, locked_at)
);

-- Dead letter queue
CREATE TABLE alert_dlq (
    id BIGSERIAL PRIMARY KEY,
    original_queue_id BIGINT,
    alert_id UUID,
    payload JSONB,
    error_history JSONB[],
    moved_to_dlq_at TIMESTAMP DEFAULT NOW(),
    retry_from_dlq BOOLEAN DEFAULT FALSE
);

-- Processing metrics for monitoring
CREATE TABLE queue_metrics (
    id BIGSERIAL PRIMARY KEY,
    metric_type VARCHAR(50),
    value NUMERIC,
    metadata JSONB,
    recorded_at TIMESTAMP DEFAULT NOW()
);
```

### 2. Producer Implementation

```typescript
// Supabase Edge Function - Producer
class QueueProducer {
    constructor(private supabase: SupabaseClient) {}
    
    async enqueueAlert(alertData: any, isNewAlert: boolean) {
        const priority = isNewAlert ? 1 : 5; // New alerts get highest priority
        
        const { error } = await this.supabase
            .from('alert_queue')
            .insert({
                trigger_type: isNewAlert ? 'new_alert' : 'bulk_upload',
                alert_id: alertData.id,
                payload: alertData,
                priority: priority,
                max_retries: isNewAlert ? 5 : 3 // More retries for individual alerts
            });
            
        if (error) throw error;
        
        // Notify consumer if high priority
        if (priority === 1) {
            await this.supabase.functions.invoke('queue-consumer-notify');
        }
    }
    
    async enqueueBatch(alerts: any[]) {
        // Split into smaller sub-batches for processing
        const BATCH_SIZE = 50;
        const batches = [];
        
        for (let i = 0; i < alerts.length; i += BATCH_SIZE) {
            const batch = alerts.slice(i, i + BATCH_SIZE);
            batches.push({
                trigger_type: 'bulk_upload',
                alert_id: `batch_${Date.now()}_${i}`,
                payload: { alerts: batch },
                priority: 5,
                max_retries: 3
            });
        }
        
        await this.supabase.from('alert_queue').insert(batches);
    }
}
```

### 3. Consumer with Adaptive Concurrency

```typescript
// Consumer Worker - Runs as scheduled job or continuous process
class AdaptiveQueueConsumer {
    private currentConcurrency = 1; // Start conservative for dev server
    private successRate = 1.0;
    private rttHistory: number[] = [];
    
    async processQueue() {
        while (true) {
            const messages = await this.dequeueMessages(this.currentConcurrency);
            
            if (messages.length === 0) {
                await this.sleep(5000); // Wait 5 seconds if queue empty
                continue;
            }
            
            const results = await Promise.allSettled(
                messages.map(msg => this.processWithCircuitBreaker(msg))
            );
            
            this.updateConcurrency(results);
        }
    }
    
    private async dequeueMessages(limit: number) {
        // Atomic dequeue with row-level locking
        const { data } = await this.supabase.rpc('dequeue_alerts', {
            worker_id: this.workerId,
            batch_size: limit
        });
        
        return data || [];
    }
    
    private updateConcurrency(results: PromiseSettledResult<any>[]) {
        const successful = results.filter(r => r.status === 'fulfilled').length;
        this.successRate = successful / results.length;
        
        if (this.successRate > 0.95) {
            // Increase concurrency (additive increase)
            this.currentConcurrency = Math.min(
                this.currentConcurrency + 1,
                3 // Max 3 for dev server, 10 for production
            );
        } else if (this.successRate < 0.8) {
            // Decrease concurrency (multiplicative decrease)
            this.currentConcurrency = Math.max(
                Math.floor(this.currentConcurrency * 0.5),
                1
            );
        }
    }
}
```

### 4. PostgreSQL Functions for Atomic Operations

```sql
-- Atomic dequeue function
CREATE OR REPLACE FUNCTION dequeue_alerts(
    worker_id TEXT,
    batch_size INT DEFAULT 5
)
RETURNS TABLE (
    id BIGINT,
    alert_id UUID,
    payload JSONB,
    retry_count INT
) AS $$
BEGIN
    RETURN QUERY
    WITH selected AS (
        SELECT q.id
        FROM alert_queue q
        WHERE q.status = 'pending'
        AND q.scheduled_for <= NOW()
        ORDER BY q.priority, q.created_at
        LIMIT batch_size
        FOR UPDATE SKIP LOCKED
    )
    UPDATE alert_queue q
    SET 
        status = 'processing',
        locked_at = NOW(),
        locked_by = worker_id
    FROM selected
    WHERE q.id = selected.id
    RETURNING q.id, q.alert_id, q.payload, q.retry_count;
END;
$$ LANGUAGE plpgsql;

-- Move to DLQ function
CREATE OR REPLACE FUNCTION move_to_dlq(queue_id BIGINT, error_msg TEXT)
RETURNS VOID AS $$
DECLARE
    queue_record RECORD;
BEGIN
    SELECT * INTO queue_record FROM alert_queue WHERE id = queue_id;
    
    INSERT INTO alert_dlq (
        original_queue_id,
        alert_id,
        payload,
        error_history
    ) VALUES (
        queue_id,
        queue_record.alert_id,
        queue_record.payload,
        ARRAY[jsonb_build_object(
            'error', error_msg,
            'timestamp', NOW(),
            'retry_count', queue_record.retry_count
        )]
    );
    
    DELETE FROM alert_queue WHERE id = queue_id;
END;
$$ LANGUAGE plpgsql;
```

### 5. Circuit Breaker & Retry Strategy

```typescript
class CircuitBreaker {
    private failureCount = 0;
    private lastFailureTime: Date | null = null;
    private state: 'CLOSED' | 'OPEN' | 'HALF_OPEN' = 'CLOSED';
    
    async execute<T>(fn: () => Promise<T>): Promise<T> {
        if (this.state === 'OPEN') {
            if (Date.now() - this.lastFailureTime!.getTime() > 60000) {
                this.state = 'HALF_OPEN';
            } else {
                throw new Error('Circuit breaker is OPEN');
            }
        }
        
        try {
            const result = await fn();
            this.onSuccess();
            return result;
        } catch (error) {
            this.onFailure();
            throw error;
        }
    }
    
    private onSuccess() {
        this.failureCount = 0;
        this.state = 'CLOSED';
    }
    
    private onFailure() {
        this.failureCount++;
        this.lastFailureTime = new Date();
        
        if (this.failureCount >= 5) {
            this.state = 'OPEN';
        }
    }
}

// Exponential backoff with jitter
function calculateDelay(retryCount: number): number {
    const baseDelay = 1000; // 1 second
    const maxDelay = 300000; // 5 minutes
    
    const exponentialDelay = Math.min(
        baseDelay * Math.pow(2, retryCount),
        maxDelay
    );
    
    // Add jitter to prevent thundering herd
    const jitter = Math.random() * 0.3 * exponentialDelay;
    
    return exponentialDelay + jitter;
}
```

## Answers to Your Specific Questions

### Queue Structure

**Q: Separate queues for different producers or Priority Queue?**

**A: Use a single priority queue.** Here's why:
- Simpler to manage (one queue, one consumer pool)
- Priority field handles urgency (new alerts = priority 1, batch = priority 5)
- Easier to monitor and scale
- Prevents queue starvation issues

### Consumer Configuration

**Q: How many consumers?**

**For Dev Server (1 CPU):**
- Start with 1 consumer process
- Use adaptive concurrency within that process (1-3 concurrent LangGraph calls)

**For Production (2 CPU, up to 10 replicas):**
- 2-3 consumer processes per replica
- Each process handles 3-5 concurrent calls
- Total: 6-15 concurrent calls across the system

**Q: Consumer assignment logic?**

Use **work-stealing pattern** with PostgreSQL's `FOR UPDATE SKIP LOCKED`:
- No pre-assignment needed
- Consumers grab work when available
- Automatic load balancing
- No coordinator required

### Concurrency Controls

**Simplified Adaptive Concurrency for your use case:**

```typescript
class SimplifiedAdaptiveConcurrency {
    private concurrency = 1;
    private recentErrors = 0;
    
    adjustConcurrency(success: boolean) {
        if (success) {
            this.recentErrors = Math.max(0, this.recentErrors - 1);
            
            // Increase every 10 successes
            if (this.recentErrors === 0) {
                this.concurrency = Math.min(this.concurrency + 1, 3);
            }
        } else {
            this.recentErrors++;
            
            // Decrease on 2 consecutive errors
            if (this.recentErrors >= 2) {
                this.concurrency = Math.max(1, Math.floor(this.concurrency * 0.7));
                this.recentErrors = 0;
            }
        }
    }
}
```

### Retry Strategy

**Centralized retry in the queue system:**
- Queue handles all retry logic
- Failed messages get rescheduled with exponential backoff
- After max_retries, move to DLQ
- No retry logic in Edge Functions

### Batch Processing Orchestration

```typescript
// For bulk uploads, create a coordinator pattern
class BatchCoordinator {
    async processBulkUpload(alerts: Alert[]) {
        // 1. Split into manageable chunks
        const chunks = this.chunkArray(alerts, 50);
        
        // 2. Enqueue with staggered scheduling
        for (let i = 0; i < chunks.length; i++) {
            await this.enqueueChunk(chunks[i], {
                priority: 5,
                scheduled_for: new Date(Date.now() + i * 5000) // 5 sec between chunks
            });
        }
        
        // 3. Monitor progress
        await this.monitorBatchProgress(batchId);
    }
}
```

## Monitoring & Migration Strategy

### Dev Server Monitoring
```sql
-- Key metrics to track
SELECT 
    status,
    COUNT(*) as count,
    AVG(retry_count) as avg_retries,
    MIN(created_at) as oldest_message
FROM alert_queue
GROUP BY status;

-- Success rate
SELECT 
    DATE_TRUNC('hour', completed_at) as hour,
    COUNT(*) FILTER (WHERE status = 'completed') as successful,
    COUNT(*) FILTER (WHERE status = 'failed') as failed,
    AVG(EXTRACT(EPOCH FROM (completed_at - created_at))) as avg_processing_time
FROM alert_queue
WHERE completed_at > NOW() - INTERVAL '24 hours'
GROUP BY hour;
```


### When to Migrate to Production

Migrate when you see:
- Queue depth consistently > 100 messages
- Processing latency > 30 seconds
- Error rate > 10%
- CPU utilization > 80% sustained

## Implementation Priority

1. **Week 1**: Implement basic queue table and enqueue logic
2. **Week 2**: Add consumer with simple concurrency (fixed at 2)
3. **Week 3**: Add circuit breaker and retry logic
4. **Week 4**: Implement adaptive concurrency
5. **Week 5**: Add monitoring and DLQ handling

# Workflow
New Alert/New Bill Insert Triggers -> Producer
Producer -> Insert to Queue
New Entry in Queue -> Consumer
Consumer: Sort by priority, scheduled for, locked at -> Designate a worker (adjust concurrency dynamically but start with 1)
Consumer -> Server: Response -> Success -> Upend Queue Processing Status to Complete 
                             -> Error -> Notify Queue Manager
Queue Manager: Circuit Breaker (which wraps retry strategy) -> Send payload back to queue
                                                            -> Move payload to DLQ






### Major feasibility notes (quick)

- **Supabase Postgres + Edge Functions**: good fit for queue table + atomic dequeue (FOR UPDATE SKIP LOCKED) + DLQ.
- **Worker execution model**: don’t rely on a single long-running Edge Function — run consumers as scheduled jobs or an external worker process that calls the dequeue RPC repeatedly. (Edge functions are fine for short tasks / event handlers.)
- **Notification**: use `pg_notify` / LISTEN (or Supabase Realtime) to wake consumers instead of busy polling whenever possible.
- **Shared state** (circuit breaker, global concurrency budget): can live in Postgres (a small `worker_state` table) if you want no extra infra; Redis makes it simpler but is optional.

### Important fixes / hardening (apply these)

1. **Centralize retry logic in Postgres** (avoid edge-function retry loops).
    - On failure, atomically `UPDATE` the row: increment `retry_count`, append to `error_history`, set `scheduled_for = now() + backoff(retry_count)`, `status='pending'`.
    - If `retry_count >= max_retries` → move to DLQ.
2. **Accumulate error history** instead of overwriting it when moving to DLQ.
3. **Avoid infinite retry loops** by relying solely on queue-managed retries and `max_retries`.
4. **Use `FOR UPDATE SKIP LOCKED`** for safe multi-consumer dequeue (you already do this — good).
5. **Gate concurrency in the consumer** (your adaptive concurrency is fine). Persist short-lived break/counter state in Postgres so multiple replicas share it.
6. **Bulk uploads**: already split into batches and stagger `scheduled_for` — good. Keep chunk sizes small (10–50) and stagger by a few seconds to avoid spikes.

### Small SQL helpers (copy/paste ready-ish pseudocode)

Reschedule on failure (safely append error and either reschedule or DLQ):

```sql
CREATE OR REPLACE FUNCTION handle_failure(queue_id BIGINT, err TEXT)
RETURNS VOID AS $$
DECLARE
  q RECORD;
  new_retry INT;
  delay_seconds INT;
BEGIN
  SELECT * INTO q FROM alert_queue WHERE id = queue_id FOR UPDATE;

  new_retry := q.retry_count + 1;
  -- exponential backoff capped at 300s (5min)
  delay_seconds := LEAST(POWER(2, new_retry)::INT * 1, 300);

  IF new_retry >= q.max_retries THEN
    -- append to error_history and move to DLQ
    INSERT INTO alert_dlq (original_queue_id, alert_id, payload, error_history, moved_to_dlq_at)
    VALUES (
      q.id, q.alert_id, q.payload,
      COALESCE(q.error_history, '[]'::jsonb) || jsonb_build_array(jsonb_build_object('error', err, 'ts', now(), 'retry', new_retry)),
      NOW()
    );
    DELETE FROM alert_queue WHERE id = queue_id;
  ELSE
    UPDATE alert_queue
    SET retry_count = new_retry,
        status = 'pending',
        scheduled_for = NOW() + (make_interval(secs => delay_seconds)),
        error_message = err,
        error_history = COALESCE(error_history, '[]'::jsonb) || jsonb_build_array(jsonb_build_object('error', err, 'ts', now(), 'retry', new_retry)),
        locked_at = NULL,
        locked_by = NULL
    WHERE id = queue_id;
  END IF;
END;
$$ LANGUAGE plpgsql;

```

### Circuit breaker & concurrency state (simple option)

Create a tiny `worker_state` table to store `failure_count`, `state`, `last_failure_at`, and `global_concurrency_limit`. Consumers read/update it transactionally; no Redis needed.

### Monitoring & alerts

- Collect queue depth, oldest `pending` timestamp, avg processing time, retry rates into `queue_metrics`.
- Alert on: queue depth growth, oldest pending > threshold, DLQ rate rising.

### Tunable defaults (recommendation)

- Dev: concurrency per process 1–3, batch 10–50, max_retries 3–5.
- Prod: per replica 3–5 concurrency, replicas 3–10 depending on load.
- Backoff cap: 5 minutes.

### Pitfalls to watch

- **Edge function cold starts / timeouts** — don’t run long synchronous waits inside them.
- **Duplicate production** — add idempotency keys in `payload` and unique constraint to avoid duplicate work.
- **Large payloads** — keep payload small (store large blobs in object storage and store URL in queue).