**TRIGGERS**
TABLES: alerts, bills

On INSERT: Trigger PRODUCER

---

**QUEUES -- FEEL GOOD ABOUT THIS**
CREATE TABLE alert_queue (
    id UUID
    trigger_type TEXT, 'new_alert' | 'bulk_bills'
    alert_id UUID
    payload JSONB
    priority INT DEFAULT 5,
    status TEXT DEFAULT 'pending'
    retry_count
    created_at
    scheduled_for
    metadata -- for failures and storing last_retry_delay for decorrelated jitter calculation
    locked_by
    locked_at
    completed_at
    error_message
)

CREATE INDEX idx_queue_pending ON alert_queue(status, priority, scheduled_for) WHERE status = 'pending';
CREATE INDEX idx_queue_locked ON alert_queue(locked_by, locked_at) WHERE status = 'processing';
CREATE INDEX idx_alert_id ON alert_queue(alert_id)

-- Consumer state (for persistence across restarts)
CREATE TABLE queue_consumer_state (
    id TEXT PRIMARY KEY DEFAULT 'singleton', -- not sure who sends this data | how this id is utilized | what the variations are
    conccurrency_level
    circuit_state
    circuit_failure_count
    circuit_last_failure
    success_rate
    last_heartbeat
    metadata
    updated_at
)

CREATE TABLE queue_metrics (
    id UUID
    metric_name
    value
    metadata
    created_at
)

-- V2 CLAUDE DIDN'T ADD BUT WE NEED
CREATE TABLE alert_dlq (
    id UUID
    original_queue_id FK to alert_queue.id
    trigger_type
    alert_id
    payload

    -- Failure Tracking
    failure_reason
    failure_count
    first_failed_at
    last_failed_at
    error_history JSONB[]

    -- Recovery tracking
    dlq_status TEXT DEFAULT 'unreviewed', -- unreviewed, investigating, wont_fix, ready_to_retry | manually change these vals
    reviewed_by TEXT,
    reviewed_at TIMESTAMPZ DEFAULT NOW(),
    notes TEXT,
    retry_from_dlq BOOLEAN

    created_at
    INDEX idx_dlq_status (dlq_status, created_at)
)

-- Function to move to DLQ
CREATE OR REPLACE FUNCTION move_to_dlq(
    p_queue_id UUID,
    p_error_message TEXT
) RETURNS void AS $$
DECLARE
    v_message RECORD;
BEGIN
    -- Get the failed message
    SELECT * INTO v_message FROM alert_queue WHERE id = p_queue_id;
    
    -- Insert into DLQ
    INSERT INTO alert_dlq (
        queue_message_id,
        source_type,
        source_id,
        payload,
        failure_reason,
        failure_count,
        first_failed_at,
        last_failed_at,
        error_history
    ) VALUES (
        v_message.id,
        v_message.source_type,
        v_message.source_id,
        v_message.payload,
        p_error_message,
        v_message.retry_count + 1,
        COALESCE(v_message.created_at, NOW()),
        NOW(),
        COALESCE(v_message.error_history, '[]'::jsonb) || 
            jsonb_build_object(
                'timestamp', NOW(),
                'error', p_error_message,
                'retry_count', v_message.retry_count
            )
    );
    
    -- Remove from main queue
    DELETE FROM alert_queue WHERE id = p_queue_id;
END;
---

**PRODUCER (Edge Function) -- WIP**
*Missing: Appropriate trigger definitions. Ensure bulk data is sent as expected for the server to process appropriately and tag bills* 

- Wait for trigger, then call functions to send message to queues.
`serve(async (req) => {
  const { type, record, old_record } = await req.json()

  // Handle different trigger types
  if (type === 'INSERT') {
    if (record.table === 'alerts') {
      await enqueueAlert(record, 'new_alert', 1) // High priority
    }
  } else if (type === 'INSERT') {
    if (record.table === 'bills' && detectBulkUpload(record, old_record)) {
      await enqueueBulkReprocess()
    }
  }

  return new Response('OK', { status: 200 })
})`

- Queueing process (Code needs modification since it doesn't fully represent how the data is sent)
`async function enqueueAlert(alert: any, source: string, priority: number) {
  const { error } = await supabase
    .from('alert_queue')
    .insert({
      source_type: source,
      source_id: alert.id,
      payload: alert,
      priority,
      // For bulk, delay to prevent thundering herd
      scheduled_for: source === 'bulk_bills'
        ? new Date(Date.now() + Math.random() * 5000).toISOString()
        : new Date().toISOString()
    })

  if (error) console.error('Enqueue error:', error)
}

async function detectBulkUpload(newRecord: any, oldRecord: any): boolean {
  // Detect if this is part of a bulk upload
  // Could check for rapid succession of updates, batch flags, etc.
  return newRecord.batch_id && newRecord.batch_id !== oldRecord.batch_id
}

async function enqueueBulkReprocess() {
  // Get all alerts that need reprocessing
  const { data: alerts } = await supabase
    .from('alerts')
    .select('id')
    .eq('needs_reprocess', true)

  // Batch them into manageable chunks
  const chunks = chunkArray(alerts, 50)

  for (let i = 0; i < chunks.length; i++) {
    await supabase.from('alert_queue').insert({
      source_type: 'bulk_bills',
      source_id: `batch_${Date.now()}_${i}`,
      payload: { alert_ids: chunks[i].map(a => a.id) },
      priority: 5,
      scheduled_for: new Date(Date.now() + i * 10000).toISOString() // Stagger by 10s
    })
  }
}`

---

**Consumer Service (Node.js on Railway) - WIP**

*Missing: Retry code with jitters not just exp. backoff, Token Buckets (when in prod)*

- Has Adaptive Concurrency adjustment, a circuit breaker
- Updates the queue table, and updates the state on the consumer_state table
- Process by batch, send heartbeat continuously, check for circuit state
- Retry uses basic exponential backoff, use jitter too
- DLQ is just a 'failed' status on queue table, should the DLQ just be a MV? or a separate table. 
    - Case for separate table is that once all processing is complete, we could retry everything from DLQ and monitor which failures persist.
- V3: DLQ Retry Strategy
`// Scheduled job or manual trigger
async function retryDLQBatch() {
  // Only retry messages marked as ready
  const { data: dlqMessages } = await supabase
    .from('alert_dlq')
    .select('*')
    .eq('dlq_status', 'ready_to_retry')
    .limit(10)
  
  for (const msg of dlqMessages) {
    // Re-queue with lower priority
    await supabase.from('alert_queue').insert({
      source_type: msg.source_type,
      source_id: msg.source_id,
      payload: msg.payload,
      priority: 10, // Lowest priority
      metadata: { 
        dlq_retry: true,
        original_dlq_id: msg.id 
      }
    })
    
    // Mark as retrying
    await supabase
      .from('alert_dlq')
      .update({ dlq_status: 'retrying' })
      .eq('id', msg.id)
  }
}`

---