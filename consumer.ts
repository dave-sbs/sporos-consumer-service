import pino from 'pino'
import 'dotenv/config'
import { createClient } from '@supabase/supabase-js'
import { MetricsCollector, createMetricsCollector, formatMetricsForDashboard } from './metrics'

const logger = pino()
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  logger.error('Environment variables SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY are missing or inaccessible.');
} else {
  logger.info('Environment variables loaded successfully.');
}

const LANGGRAPH_API_URL = process.env.LANGGRAPH_API_URL!
const LANGGRAPH_API_KEY = process.env.LANGGRAPH_API_KEY!
const LANGGRAPH_ASSISTANT_ID = process.env.LANGGRAPH_ASSISTANT_ID!

if (!LANGGRAPH_API_URL || !LANGGRAPH_API_KEY || !LANGGRAPH_ASSISTANT_ID) {
  logger.error('Environment variables LANGGRAPH_API_URL, LANGGRAPH_API_KEY, or LANGGRAPH_ASSISTANT_ID are missing or inaccessible.');
} else {
  logger.info('Environment variables loaded successfully.');
}

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

interface AlertPayload {
    id: string, // uuid
    user_id: string, // uuid
    alert_name: string,
    normalized_status: string,
    created_at: Date,
    updated_at: Date,
    last_bill_search_at: Date,
    bill_search_attempt_count: number,
    bill_search_status: 'pending' | 'success' | 'failed',
    alert_priority: 'low' | 'high',
}

interface AlertsQueue {
    id: string, // uuid
    trigger_type: 'new_alert' | 'bulk_alerts',
    alert_id: string, // uuid
    payload: AlertPayload,
    priority: number,
    status: 'pending' | 'processing' | 'completed' | 'failed',
    retry_count: number,
    created_at: Date,
    scheduled_for: Date,
    locked_by: string,
    locked_at: Date,
    completed_at: Date,
    error: any,
    metadata: Record<string, any>,
}

interface AlertsDLQTable {
    original_queue_id: string, // uuid
    trigger_type: 'new_alert' | 'bulk_alerts',
    alert_id: string, // uuid
    payload: any,
    failure_reason: string,
    failure_count: number,
    first_failed_at: Date,
    last_failed_at: Date,
    error_history: any[],
    dlq_status: 'unreviewed' | 'investigating' | 'wont_fix' | 'ready_to_retry',
    reviewed_by: string,
    reviewed_at: Date,
    notes: string,
    created_at: Date;
}

interface QueueConsumerState {
    concurrency_level: number;
    circuit_state: 'CLOSED' | 'OPEN' | 'HALF_OPEN';
    circuit_failure_count: number;
    circuit_last_failure: Date | null;
    success_rate: number;
    last_heartbeat: Date;
    metadata: Record<string, any>;
    updated_at: Date;
}


class QueueConsumer {
    private concurrency = 1;
    private minConcurrency = 1;
    private maxConcurrency = 5;
    private consumerId = `consumer-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    private successCount = 0;
    private errorCount = 0;
    private lastAdjustment = Date.now();
    private adjustmentInterval = 30000; // 30 seconds
    private successThreshold = 0.95; // 95% success rate to increase concurrency
    private maxRetries = 3;
    private isServerless = false; // Enable serverless mode
    private idleTimeout = 30000; // 30 seconds of no work before shutdown
    private lastWorkTime = Date.now();
    private enableHttpServer = false; // Set to true if you want HTTP wake-up endpoint
    private lastMetricsLog = Date.now();

    // Debug non-matches
    private nonMatchedResponses: Record<string, any> = {};
    
    // Metrics collector
    private metrics: MetricsCollector;
    
    private circuitBreaker = {
        state: 'CLOSED' as 'CLOSED' | 'OPEN' | 'HALF_OPEN',
        failures: 0,
        lastFailure: null as Date | null,
        threshold: 5,
        timeout: 60000, // 1 minute
        halfOpenMaxCalls: 3,
        halfOpenCalls: 0
    }

    async start() {
        logger.info(`Consumer starting with ID: ${this.consumerId} in ${this.isServerless ? 'serverless' : 'continuous'} mode`)

        // Initialize metrics collector
        this.metrics = createMetricsCollector()

        // Restore state from DB
        await this.restoreState() 
        
        // Start HTTP server if enabled (for wake-up calls)
        if (this.enableHttpServer) {
            this.startHttpServer()
        }
        
        if (this.isServerless) {
            await this.runServerless()
        } else {
            await this.runContinuous()
        }
    }

    private startHttpServer(): void {
        // Simple HTTP server for wake-up calls
        const http = require('http')
        const server = http.createServer((req: any, res: any) => {
            if (req.method === 'POST' && req.url === '/wake') {
                logger.info('Received wake-up call from edge function')
                res.writeHead(200, { 'Content-Type': 'application/json' })
                res.end(JSON.stringify({ status: 'awake', consumerId: this.consumerId }))
                
                // Wake up the consumer
                this.wakeUp()
            } else if (req.method === 'GET' && req.url === '/health') {
                res.writeHead(200, { 'Content-Type': 'application/json' })
                res.end(JSON.stringify({ 
                    status: 'healthy', 
                    consumerId: this.consumerId,
                    mode: this.isServerless ? 'serverless' : 'continuous',
                    idleTime: Date.now() - this.lastWorkTime
                }))
            } else if (req.method === 'GET' && req.url === '/metrics') {
                res.writeHead(200, { 'Content-Type': 'application/json' })
                const summary = this.metrics.getMetricsSummary()
                const formatted = formatMetricsForDashboard(summary)
                res.end(JSON.stringify(formatted, null, 2))
            } else if (req.method === 'GET' && req.url === '/metrics/raw') {
                res.writeHead(200, { 'Content-Type': 'application/json' })
                res.end(JSON.stringify(this.metrics.getMetricsSummary(), null, 2))
            } else if (req.method === 'GET' && req.url === '/metrics/prometheus') {
                res.writeHead(200, { 'Content-Type': 'text/plain' })
                res.end(this.metrics.exportMetricsForPrometheus())
            } else {
                res.writeHead(404)
                res.end('Not found')
            }
        })
        
        const port = process.env.PORT || 3000
        server.listen(port, () => {
            logger.info(`HTTP server listening on port ${port}`)
        })
    }

    private async runServerless(): Promise<void> {
        logger.info('Running in serverless mode - will shutdown when idle')
        
        while (true) {
            try {
                if (this.canProcess()) {
                    const hasWork = await this.processNextBatch()
                    if (hasWork) {
                        this.lastWorkTime = Date.now()
                        logger.info('Work found, resetting idle timer')
                    } else {
                        // Check if we've been idle long enough to shutdown
                        const idleTime = Date.now() - this.lastWorkTime
                        if (idleTime > this.idleTimeout) {
                            logger.info(`Idle for ${idleTime}ms, shutting down serverless consumer`)
                            logger.info('Updating circuit breaker to reset state and failure count before idling')
                            this.updateCircuitBreakerBeforeIdle()
                            console.log(`Non-matches: ${JSON.stringify(this.nonMatchedResponses)}`);
                            await this.shutdown()
                            process.exit(0)
                        } else {
                            logger.debug(`Idle for ${idleTime}ms, will shutdown in ${this.idleTimeout - idleTime}ms`)
                        }
                    }
                } else {
                    logger.info('Circuit breaker OPEN, waiting...')
                    await this.sleep(this.circuitBreaker.timeout / 4)
                }
                
                await this.sendHeartbeat()
                await this.adjustConcurrency()
            } catch (error) {
                logger.error({ error }, 'Processing error')
                await this.handleError(error)
            }

            await this.sleep(1000)
        }
    }

    private async runContinuous(): Promise<void> {
        logger.info('Running in continuous mode - will run indefinitely')
        
        while (true) {
            try {
                if (this.canProcess()) {
                    await this.processNextBatch()
                } else {
                    logger.info('Circuit breaker OPEN, waiting...')
                    await this.sleep(this.circuitBreaker.timeout / 4)
                }
                
                await this.sendHeartbeat()
                await this.adjustConcurrency()
            } catch (error) {
                logger.error({ error }, 'Processing error')
                await this.handleError(error)
            }

            await this.sleep(1000)
        }
    }

    // DB Persistence Methods
    private async restoreState(): Promise<void> {
        try {
            const { data, error } = await supabase
                .from('alerts_queue_consumer_state')
                .select('*')
                .single()
            
            if (error && error.code !== 'PGRST116') { // PGRST116 = no rows returned
                logger.error({ error }, 'Error fetching consumer state')
                return
            }

            if (data) {
                this.concurrency = data.concurrency_level || 1
                this.circuitBreaker.state = data.circuit_state || 'CLOSED'
                this.circuitBreaker.failures = data.circuit_failure_count || 0
                this.circuitBreaker.lastFailure = data.circuit_last_failure ? new Date(data.circuit_last_failure) : null
                logger.info({ state: data }, 'Restored consumer state from database')
            } else {
                logger.info('No existing consumer state found, starting with defaults')
                await this.persistState()
            }
        } catch (error) {
            logger.error({ error }, 'Failed to restore state, using defaults')
        }
    }

    private async persistState(): Promise<void> {
        try {
            const state: QueueConsumerState = {
                concurrency_level: this.concurrency,
                circuit_state: this.circuitBreaker.state,
                circuit_failure_count: this.circuitBreaker.failures,
                circuit_last_failure: this.circuitBreaker.lastFailure,
                success_rate: this.getSuccessRate(),
                last_heartbeat: new Date(),
                metadata: {
                    consumer_id: this.consumerId,
                    success_count: this.successCount,
                    error_count: this.errorCount
                },
                updated_at: new Date()
            }

            await supabase
                .from('alerts_queue_consumer_state')
                .upsert(state)
        } catch (error) {
            logger.error({ error }, 'Failed to persist state')
        }
    }

    // Processing Methods
    private async processNextBatch(): Promise<boolean> {
        const messages = await this.fetchNextBatch()
        if (messages.length === 0) {
            await this.sleep(2000) // No messages, wait longer
            return false
        }

        logger.info(`Processing batch of ${messages.length} messages with concurrency ${this.concurrency}`)

        // Record batch size for metrics
        this.metrics.recordBatchSize(messages.length)

        // Process messages with limited concurrency
        const promises = messages.map(message => this.processMessage(message))
        const results = await Promise.allSettled(promises)
        results.forEach((result, index) => {
            if (result.status === 'rejected') {
                logger.error({ error: result.reason, messageId: messages[index].id }, 'Unexpected message processing rejection')
            }
        })
        return true
    }

    private async fetchNextBatch(): Promise<AlertsQueue[]> {
        try {
            const lockExpiry = new Date(Date.now() - 5 * 60 * 1000) // 5 minutes ago
            
            const { data, error } = await supabase
                .from('alerts_queue')
                .select('*')
                .eq('status', 'pending')
                .or(`locked_by.is.null,locked_at.lt.${lockExpiry.toISOString()}`)
                .order('priority', { ascending: false })
                .order('created_at', { ascending: true })
                .limit(this.concurrency)

            if (error) {
                logger.error({ error }, 'Failed to fetch messages')
                return []
            }

            // Lock the messages
            if (data && data.length > 0) {
                const messageIds = data.map(m => m.id)
                await supabase
                    .from('alerts_queue')
                    .update({
                        status: 'processing',
                        locked_by: this.consumerId,
                        locked_at: new Date().toISOString()
                    })
                    .in('id', messageIds)
            }

            return data || []
        } catch (error) {
            logger.error({ error }, 'Error fetching batch')
            return []
        }
    }

    private async processMessage(message: AlertsQueue): Promise<void> {
        const startKey = this.metrics.recordProcessingStart(message.id)
        let matchCount = 0
        
        try {
            logger.info(`Processing message: ${message.id}, for alert: ${message.payload.alert_name}`)

            // Process the alert and get match count
            matchCount = await this.processAlert(message.payload)

            // Mark as completed
            await this.markMessageCompleted(message.id)
            this.successCount++
            
            // Record successful processing with metrics
            this.metrics.recordProcessingEnd(
                startKey, 
                true, 
                matchCount, 
                message.payload.id,
                message.payload.alert_name,
                message.payload.alert_priority
            )
            
            // Update circuit breaker on success
            if (this.circuitBreaker.state === 'HALF_OPEN') {
                this.circuitBreaker.halfOpenCalls++
                if (this.circuitBreaker.halfOpenCalls >= this.circuitBreaker.halfOpenMaxCalls) {
                    this.circuitBreaker.state = 'CLOSED'
                    this.circuitBreaker.failures = 0
                    this.circuitBreaker.halfOpenCalls = 0
                    this.metrics.recordCircuitBreakerEvent('closed', 'CLOSED', 'HALF_OPEN')
                    logger.info('Circuit breaker closed after successful half-open calls')
                }
            }

        } catch (error) {
            logger.error({ error, messageId: message.id }, 'Failed to process message')
            
            // Record failed processing
            this.metrics.recordProcessingEnd(startKey, false, 0)
            this.metrics.recordError(error, {
                alertId: message.payload.id,
                retryCount: message.retry_count
            })
            
            await this.handleMessageError(message, error)
            this.errorCount++
            this.updateCircuitBreakerOnFailure()
        }
    }

    private async processAlert(payload: AlertPayload): Promise<number> {
        try {
            // sendToLangGraph now handles all error validation internally
            const result = await this.sendToLangGraph(payload);

            // extractMatches now handles response validation and proper error detection
            const matches = await this.extractMatches(result);
            
            if (matches.length > 0) {
                await this.addMatchedBills(payload.id, matches);
                logger.info(`Successfully processed alert ${payload.id}: ${matches.length} matches found and stored`);
            } else {
                logger.info(`Successfully processed alert ${payload.id}: no matches found`);
            }
            
            return matches.length;
            
        } catch (error) {
            logger.error({ error: error.message, alertId: payload.id, alertName: payload.alert_name }, 'Alert processing failed');
            throw error;
        }
    }

    private async sendToLangGraph(payload: AlertPayload): Promise<any> {
        const langgraph_payload = {
            assistant_id: LANGGRAPH_ASSISTANT_ID,
            thread_id: `alert_${payload.id}`,
            input: {
            query: payload.alert_name   
            }
        };

        const headers = {
            'Content-Type': 'application/json'
        };

        if (LANGGRAPH_API_KEY) {
            headers['x-api-key'] = LANGGRAPH_API_KEY;
        }

        const controller = new AbortController();
        const timeout = setTimeout(()=>controller.abort(), 10000);
        const startTime = Date.now();
        
        try {
            const response = await fetch(`${LANGGRAPH_API_URL}/runs/wait`, {
                method: 'POST',
                headers,
                body: JSON.stringify(langgraph_payload),
                signal: controller.signal
            });

            if (!response.ok) {
                const errorText = await response.text();
                logger.error({ status: response.status, errorText }, 'LangGraph HTTP error response');
                throw new Error(`LangGraph HTTP error: ${response.status} - ${errorText}`);
            }

            // Check if response is complete
            const contentLength = response.headers.get('content-length');
            if (contentLength && contentLength === '0') {
                throw new Error('LangGraph returned empty response (content-length: 0)');
            }

            let result;
            try {
                result = await response.json();
            } catch (jsonError) {
                logger.error({ jsonError, status: response.status }, 'Failed to parse LangGraph response as JSON');
                throw new Error(`LangGraph response parsing failed: ${jsonError.message}`);
            }

            // Validate response structure
            if (!result || typeof result !== 'object') {
                logger.error({ result }, 'LangGraph returned invalid response structure');
                throw new Error('LangGraph returned invalid response structure (not an object)');
            }

            // Check for LangGraph-specific error indicators
            if (result.status === 'error' || result.error) {
                const errorMsg = result.error || result.message || 'Unknown LangGraph error';
                logger.error({ error: result }, 'LangGraph returned error status');
                throw new Error(`LangGraph processing error: ${errorMsg}`);
            }

            // Additional validation for expected response structure
            if (!result.hasOwnProperty('retrieved_docs') && !result.hasOwnProperty('query')) {
                logger.warn({ result }, 'LangGraph response missing expected fields (retrieved_docs or query)');
                // Don't throw here as it might be a valid response format we haven't seen
            }
            
            // Record successful LangGraph timing
            this.metrics.recordLangGraphTiming(Date.now() - startTime);
            
            return result;
        } catch (error) {
            // Record failed LangGraph timing
            this.metrics.recordLangGraphTiming(Date.now() - startTime);
            
            if (error.name === 'AbortError') {
                logger.warn('LangGraph request timed out after 10 seconds');
                throw new Error('LangGraph request timed out after 10 seconds');
            }
            
            // Enhanced error logging for connection issues
            if (error.message?.includes('fetch') || error.message?.includes('network') || 
                error.message?.includes('ECONNRESET') || error.message?.includes('EPIPE')) {
                logger.error({ error: error.message, alertId: payload.id }, 'LangGraph network/connection error detected');
                throw new Error(`LangGraph connection error: ${error.message}`);
            }
            
            logger.error({ error: error.message, alertId: payload.id }, 'LangGraph request failed');
            throw error;
        }
        finally{
            clearTimeout(timeout);
        }
    }

    // Extract matches from LangGraph response
    private async extractMatches(response: any): Promise<{bill_id: string}[]> {
        // Validate response structure first
        if (!response || typeof response !== 'object') {
            logger.error({ response }, 'Invalid response structure in extractMatches');
            throw new Error('Cannot extract matches from invalid response structure');
        }

        const matches: any[] = [];
        
        // Check if retrieved_docs exists and is an array
        if (response.retrieved_docs) {
            if (!Array.isArray(response.retrieved_docs)) {
                logger.error({ retrieved_docs: response.retrieved_docs }, 'retrieved_docs is not an array');
                throw new Error('LangGraph response.retrieved_docs is not an array');
            }

            for (const doc of response.retrieved_docs) {
                if (!doc || !doc.id) {
                    logger.warn({ doc }, 'Retrieved document missing id field');
                    continue; // Skip malformed docs but don't fail entirely
                }
                matches.push({
                    bill_id: doc.id
                });
            }
        } else {
            // Check if this is a valid response with no matches vs a malformed response
            if (!response.hasOwnProperty('retrieved_docs')) {
                // If response doesn't have retrieved_docs at all, check if it has other expected fields
                if (!response.hasOwnProperty('query') && Object.keys(response).length === 0) {
                    logger.error({ response }, 'Response appears to be empty or malformed - no expected fields found');
                    throw new Error('LangGraph response appears to be empty or malformed');
                }
                // Log warning but don't fail - might be a valid response format
                logger.warn({ response }, 'Response missing retrieved_docs field but has other data');
            }
        }

        // Log the result with more context
        if (matches.length > 0) {
            logger.info(`Matches found: ${matches.length}`);
        } else {
            // Check if this is a legitimate "no matches" or a potential error
            if (response.retrieved_docs && Array.isArray(response.retrieved_docs) && response.retrieved_docs.length === 0) {
                logger.info('No matches found - retrieved_docs is empty array (legitimate no-match result)');
            } else if (!response.retrieved_docs && response.query) {
                logger.info('No matches found - no retrieved_docs but query present (legitimate no-match result)');
            } else {
                logger.warn({ response }, 'No matches found - response structure may be unexpected');
            }
        }

        // Store non-matched responses for debugging (only if it looks like a legitimate response)
        if (matches.length === 0 && (response.query || response.retrieved_docs !== undefined)) {
            this.nonMatchedResponses[response.query || 'unknown_query'] = response;
        }

        return matches;
    }

    private async addMatchedBills(alertId: string, billMatches: {bill_id: string}[]): Promise<void> {
        const alertBills = billMatches.map(bill => ({
            alert_id: alertId,
            bill_id: bill.bill_id,
            matched_date: new Date().toISOString()
        }));

        const { error } = await supabase.from('alert_bills').upsert(alertBills, {
            onConflict: 'alert_id,bill_id',
            ignoreDuplicates: true
          });
          if (error) {
            console.error('Failed to store matches:', error);
            throw error;
          }
        logger.info(`Added ${billMatches.length} bills to alert ${alertId}`)
    }
    
    private async markMessageCompleted(messageId: string): Promise<void> {
        try {
            const { error } = await supabase
            .from('alerts_queue')
            .update({
                status: 'completed',
                completed_at: new Date().toISOString(),
                locked_by: null,
                locked_at: null
            })
            .eq('id', messageId)

            if (error) {
                logger.error({ error }, 'Failed to mark message as completed')
                throw error;
            }
        } catch (error) {
            logger.error({ error }, 'Failed to mark message as completed')
            throw error;
        }
    }

    private async handleMessageError(message: AlertsQueue, error: any): Promise<void> {
        const retryCount = message.retry_count + 1

        if (retryCount >= this.maxRetries) {
            // Move to DLQ
            await this.moveToDLQ(message, error)
            await this.markMessageFailed(message.id)
            this.metrics.recordDLQEntry(error.message || String(error))
        } else {
            // Schedule retry with jitter
            const delay = this.calculateRetryDelay(retryCount)
            const scheduledFor = new Date(Date.now() + delay)

            await supabase
                .from('alerts_queue')
                .update({
                    status: 'pending',
                    retry_count: retryCount,
                    scheduled_for: scheduledFor.toISOString(),
                    error: error.message || String(error),
                    locked_by: null,
                    locked_at: null
                })
                .eq('id', message.id)

            logger.info(`Scheduled retry ${retryCount}/${this.maxRetries} for message ${message.id} in ${delay}ms`)
        }
    }

    private async moveToDLQ(message: AlertsQueue, error: any): Promise<void> {
        try {
            const dlqEntry: Partial<AlertsDLQTable> = {
                original_queue_id: message.id,
                trigger_type: message.trigger_type,
                alert_id: message.alert_id,
                payload: message.payload,
                failure_reason: error.message || String(error),
                failure_count: message.retry_count + 1,
                first_failed_at: new Date(),
                last_failed_at: new Date(),
                error_history: [{ error: error.message || String(error), timestamp: new Date() }],
                dlq_status: 'unreviewed',
                created_at: new Date()
            }

            await supabase.from('alerts_dlq').insert(dlqEntry)
            logger.warn(`Moved message ${message.id} to DLQ after ${message.retry_count + 1} failures`)
        } catch (dlqError) {
            logger.error({ dlqError, messageId: message.id }, 'Failed to move message to DLQ')
        }
    }

    private async markMessageFailed(messageId: string): Promise<void> {
        await supabase
            .from('alerts_queue')
            .update({
                status: 'failed',
                completed_at: new Date().toISOString(),
                locked_by: null,
                locked_at: null
            })
            .eq('id', messageId)
    }

    private calculateRetryDelay(retryCount: number): number {
        // Exponential backoff with jitter
        const baseDelay = Math.min(1000 * Math.pow(2, retryCount - 1), 30000) // Max 30 seconds
        const jitter = Math.random() * 0.5 * baseDelay // Up to 50% jitter
        return baseDelay + jitter
    }

    private updateCircuitBreakerOnFailure(): void {
        const oldState = this.circuitBreaker.state
        this.circuitBreaker.failures++
        this.circuitBreaker.lastFailure = new Date()

        if (this.circuitBreaker.failures >= this.circuitBreaker.threshold) {
            this.circuitBreaker.state = 'OPEN'
            this.circuitBreaker.halfOpenCalls = 0
            this.metrics.recordCircuitBreakerEvent('opened', 'OPEN', oldState)
            logger.warn(`Circuit breaker opened after ${this.circuitBreaker.failures} failures`)
        }
    }

    private updateCircuitBreakerBeforeIdle(): void {
        this.circuitBreaker.state = 'CLOSED'
        this.circuitBreaker.failures = 0
        this.circuitBreaker.halfOpenCalls = 0

        supabase
            .from('alerts_queue_consumer_state')
            .update({
                circuit_state: this.circuitBreaker.state,
                circuit_failure_count: this.circuitBreaker.failures,
            })
            .eq('id', 'singleton')
    }

    private async sendHeartbeat(): Promise<void> {
        try {
            await this.persistState()
            
            // Log metrics every 5 minutes
            if (Date.now() - this.lastMetricsLog > 5 * 60 * 1000) {
                this.metrics.logMetricsSummary()
                this.lastMetricsLog = Date.now()
            }
            
            logger.debug('Heartbeat sent')
        } catch (error) {
            logger.error({ error }, 'Failed to send heartbeat')
        }
    }

    private async adjustConcurrency(): Promise<void> {
        const now = Date.now()
        if (now - this.lastAdjustment < this.adjustmentInterval) {
            return
        }

        const totalRequests = this.successCount + this.errorCount
        if (totalRequests < 10) {
            return // Need more data points
        }

        const successRate = this.getSuccessRate()
        const currentConcurrency = this.concurrency

        logger.info(`Success rate: ${successRate}, Current concurrency: ${currentConcurrency}`)

        if (successRate >= this.successThreshold && this.concurrency < this.maxConcurrency) {
            // Increase concurrency
            this.concurrency = Math.min(this.concurrency + 1, this.maxConcurrency)
            this.metrics.recordConcurrencyChange(currentConcurrency, this.concurrency, 'increased_due_to_high_success_rate')
            logger.info(`Increased concurrency from ${currentConcurrency} to ${this.concurrency} (success rate: ${(successRate * 100).toFixed(2)}%)`)
        } else if (successRate < 0.8 && this.concurrency > this.minConcurrency) {
            // Decrease concurrency
            this.concurrency = Math.max(this.concurrency - 1, this.minConcurrency)
            this.metrics.recordConcurrencyChange(currentConcurrency, this.concurrency, 'decreased_due_to_low_success_rate')
            logger.info(`Decreased concurrency from ${currentConcurrency} to ${this.concurrency} (success rate: ${(successRate * 100).toFixed(2)}%)`)
        }

        // Reset counters
        this.successCount = 0
        this.errorCount = 0
        this.lastAdjustment = now
    }

    private getSuccessRate(): number {
        const total = this.successCount + this.errorCount
        return total === 0 ? 1 : this.successCount / total
    }

    private async handleError(error: any): Promise<void> {
        logger.error({ error }, 'Consumer error')
        this.updateCircuitBreakerOnFailure()
        await this.sleep(5000) // Wait before retrying
    }

    // Utility Methods
    private async sleep(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms))
    }

    private canProcess(): boolean {
        if (this.circuitBreaker.state === 'CLOSED') return true

        // If circuit is OPEN, check if enough time has passed and set to HALF_OPEN
        if (this.circuitBreaker.state === 'OPEN') {
            const timeSinceFailure = Date.now() - this.circuitBreaker.lastFailure!.getTime()
            if (timeSinceFailure > this.circuitBreaker.timeout) {
                this.circuitBreaker.state = 'HALF_OPEN'
                this.circuitBreaker.halfOpenCalls = 0
                this.metrics.recordCircuitBreakerEvent('half_opened', 'HALF_OPEN', 'OPEN')
                logger.info('Circuit breaker half-open, trying again...')
                return true
            }
            return false // Still OPEN
        }

        return true // HALF_OPEN
    }

    // Graceful shutdown method
    async shutdown(): Promise<void> {
        logger.info('Starting graceful shutdown...')
        
        // Wait for current processing to complete
        // In a real implementation, you'd track active promises and wait for them
        await this.sleep(2000)
        
        // Final state persistence
        await this.persistState()
        
        logger.info('Consumer shutdown complete')
    }

    // Method to wake up consumer (called by edge function)
    public wakeUp(): void {
        logger.info('Consumer woken up by external call')
        this.lastWorkTime = Date.now()
    }
}

// Start consumer
const consumer = new QueueConsumer()

// Configuration from environment variables
const config = {
    serverless: process.env.CONSUMER_MODE === 'serverless' ? true : false,
    idleTimeout: parseInt(process.env.IDLE_TIMEOUT || '30000'),
    enableHttp: process.env.ENABLE_HTTP === 'true' ? true : false,
    port: process.env.PORT || 3000
}

// Apply configuration
consumer['isServerless'] = config.serverless
consumer['idleTimeout'] = config.idleTimeout
consumer['enableHttpServer'] = config.enableHttp

logger.info({ config }, 'Consumer configuration loaded')

consumer.start().catch(error => {
    logger.fatal({ error }, 'Consumer crashed')
    process.exit(1)
})

// Graceful shutdown
process.on('SIGTERM', async () => {
    logger.info('Received SIGTERM, shutting down gracefully...')
    await consumer.shutdown()
    process.exit(0)
})

process.on('SIGINT', async () => {
    logger.info('Received SIGINT, shutting down gracefully...')
    await consumer.shutdown()
    process.exit(0)
})