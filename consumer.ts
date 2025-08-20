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
    private maxConcurrency = 3; // 3 is max conccurency it can handle in dev
    private consumerId = `consumer-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    private successCount = 0;
    private errorCount = 0;
    private lastAdjustment = Date.now();
    private adjustmentInterval = 30000; // 30 seconds
    private successThreshold = 0.95; // 95% success rate to increase concurrency
    private maxRetries = 3;
    private maxRetriesBrokenPipe = 7;
    private isServerless = true; // Enable serverless mode
    private idleTimeout = 30000; // 30 seconds of no work before shutdown
    private lastWorkTime = Date.now();
    private enableHttpServer = false; // Will be set from environment variables
    private lastMetricsLog = Date.now();
    private isWorkerSleeping = false; // Track if worker is sleeping

    // Debug non-matches
    private nonMatchedResponses: Record<string, any> = {};
    
    // Metrics collector
    private metrics!: MetricsCollector;
    
    // Broken pipe specific handling
    private brokenPipeState = {
        isInPanicMode: false,
        consecutiveBrokenPipes: 0,
        lastBrokenPipeTime: null as Date | null,
        panicModeStartTime: null as Date | null,
        originalConcurrency: 1,
        brokenPipeCount: 0,
        recoveryAttempts: 0
    };
    
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
            await this.runServerlessWithKeepAlive()
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
                    workerStatus: this.isWorkerSleeping ? 'sleeping' : 'active',
                    idleTime: Date.now() - this.lastWorkTime,
                    httpServerActive: true
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
            } else if (req.method === 'GET' && req.url === '/') {
                res.writeHead(200, { 'Content-Type': 'application/json' })
                res.end(JSON.stringify({ 
                    status: 'ok', 
                    service: 'sporos-consumer-service',
                    mode: 'serverless',
                    timestamp: new Date().toISOString(),
                    consumerId: this.consumerId
                }))
            } else {
                res.writeHead(404)
                res.end('Not found')
            }
        })
        
        const port = process.env.PORT
        if (!port) {
            throw new Error('PORT environment variable is required but not set')
        }
        
        const host = '0.0.0.0' // Explicitly bind to all interfaces for Railway
        server.listen(parseInt(port), host, () => {
            logger.info(`HTTP server listening on ${host}:${port}`)
            logger.info(`Server accessible at: http://${host}:${port}`)
            logger.info('Available endpoints:')
            logger.info('  POST /wake - Wake up the consumer')
            logger.info('  GET /health - Health check')
            logger.info('  GET /metrics - Formatted metrics')
            logger.info('  GET /metrics/raw - Raw metrics data')
            logger.info('  GET /metrics/prometheus - Prometheus format')
        })
    }

    private async runServerlessWithKeepAlive(): Promise<void> {
        logger.info('Starting serverless mode with HTTP server keep-alive')
        
        // Start the worker
        await this.runWorkerUntilSleep()
        
        // Worker is now sleeping, but HTTP server stays alive
        logger.info('Worker sleeping, HTTP server remains active for wake-up calls')
        
        // Keep the process alive with minimal activity
        while (true) {
            if (!this.isWorkerSleeping) {
                // Worker was woken up, restart it
                logger.info('Worker woken up, restarting processing loop')
                await this.runWorkerUntilSleep()
                logger.info('Worker returned to sleep, HTTP server remains active')
            }
            
            // Minimal heartbeat while sleeping
            await this.sleep(5000) // Check every 5 seconds if worker should restart
        }
    }

    private async runWorkerUntilSleep(): Promise<void> {
        logger.info('Worker starting processing loop')
        this.isWorkerSleeping = false
        
        while (!this.isWorkerSleeping) {
            try {
                if (this.canProcess()) {
                    const hasWork = await this.processNextBatch()
                    if (hasWork) {
                        this.lastWorkTime = Date.now()
                        logger.info('Work found, resetting idle timer')
                    } else {
                        // Check if we've been idle long enough to sleep worker
                        const idleTime = Date.now() - this.lastWorkTime
                        if (idleTime > this.idleTimeout) {
                            logger.info(`Idle for ${idleTime}ms, putting worker to sleep (HTTP server stays alive)`)
                            await this.enterSleepMode()
                            return // Exit worker loop but keep process alive
                        } else {
                            logger.debug(`Idle for ${idleTime}ms, will sleep worker in ${this.idleTimeout - idleTime}ms`)
                        }
                    }
                } else {
                    logger.info('Circuit breaker OPEN, waiting...')
                    await this.sleep(this.circuitBreaker.timeout / 4)
                }
                
                await this.sendHeartbeat()
                await this.checkBrokenPipeRecovery()
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
                await this.checkBrokenPipeRecovery()
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
            
            // Reset broken pipe counter on successful processing
            this.onSuccessfulProcessing()
            
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
            const errorMessage = error instanceof Error ? error.message : String(error);
            logger.error({ error: errorMessage, alertId: payload.id, alertName: payload.alert_name }, 'Alert processing failed');
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

        const headers: Record<string, string> = {
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
                const jsonErrorMessage = jsonError instanceof Error ? jsonError.message : String(jsonError);
                logger.error({ jsonError: jsonErrorMessage, status: response.status }, 'Failed to parse LangGraph response as JSON');
                throw new Error(`LangGraph response parsing failed: ${jsonErrorMessage}`);
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
            
            const errorMessage = error instanceof Error ? error.message : String(error);
            const errorName = error instanceof Error ? error.name : 'UnknownError';
            
            if (errorName === 'AbortError') {
                logger.warn('LangGraph request timed out after 10 seconds');
                throw new Error('LangGraph request timed out after 10 seconds');
            }
            
            // Enhanced error logging for connection issues
            if (errorMessage.includes('fetch') || errorMessage.includes('network') || 
                errorMessage.includes('ECONNRESET') || errorMessage.includes('EPIPE')) {
                logger.error({ error: errorMessage, alertId: payload.id }, 'LangGraph network/connection error detected');
                throw new Error(`LangGraph connection error: ${errorMessage}`);
            }
            
            logger.error({ error: errorMessage, alertId: payload.id }, 'LangGraph request failed');
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

        // Check for LangGraph error responses first
        if (response.__error__) {
            const errorData = response.__error__;
            const errorMessage = errorData.message || errorData.error || 'Unknown LangGraph error';
            logger.error({ errorData }, 'LangGraph returned error response');
            throw new Error(`LangGraph error: ${errorMessage}`);
        }

        // Check for other error indicators
        if (response.error) {
            logger.error({ error: response.error }, 'LangGraph returned error field');
            throw new Error(`LangGraph error: ${response.error}`);
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
                
                // More aggressive check for unexpected response structure
                const responseKeys = Object.keys(response);
                const expectedKeys = ['retrieved_docs', 'query', 'status', 'message'];
                const hasAnyExpectedKey = responseKeys.some(key => expectedKeys.includes(key));
                
                if (!hasAnyExpectedKey && responseKeys.length > 0) {
                    logger.error({ response, responseKeys }, 'Response has unexpected structure - no recognized fields');
                    throw new Error(`LangGraph response has unexpected structure with keys: ${responseKeys.join(', ')}`);
                }
                
                // Log warning but don't fail - might be a valid response format
                logger.warn({ response }, 'Response missing retrieved_docs field but has other recognized data');
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
        // Check if this is a broken pipe error and handle specially
        if (this.isBrokenPipeError(error)) {
            await this.handleBrokenPipeError(message, error);
            return;
        }

        // Standard error handling for non-broken-pipe errors
        const retryCount = message.retry_count + 1

        if (retryCount >= this.maxRetries) {
            // Move to DLQ
            await this.moveToDLQ(message, error)
            await this.markMessageFailed(message.id)
            this.metrics.recordDLQEntry(error.message || String(error))
        } else {
            // Schedule retry with standard jitter
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

    // Broken Pipe Detection and Handling
    private isBrokenPipeError(error: any): boolean {
        const errorStr = error.message || error.toString() || '';
        return errorStr.includes('[Errno 32] Broken pipe') || 
               errorStr.includes('EPIPE') ||
               errorStr.includes('Broken pipe') ||
               errorStr.includes('Connection reset by peer') ||
               errorStr.includes('WriteError') ||
               errorStr.includes('RemoteProtocolError') ||
               errorStr.includes('ConnectionTerminated');
    }

    private async handleBrokenPipeError(message: AlertsQueue, error: any): Promise<void> {
        this.brokenPipeState.brokenPipeCount++;
        this.brokenPipeState.consecutiveBrokenPipes++;
        this.brokenPipeState.lastBrokenPipeTime = new Date();

        logger.error({ 
            error: error.message, 
            messageId: message.id,
            consecutiveBrokenPipes: this.brokenPipeState.consecutiveBrokenPipes,
            totalBrokenPipes: this.brokenPipeState.brokenPipeCount
        }, 'Broken pipe error detected');

        // Enter panic mode if we hit multiple broken pipes
        if (this.brokenPipeState.consecutiveBrokenPipes >= 2 && !this.brokenPipeState.isInPanicMode) {
            await this.enterBrokenPipePanicMode();
        }

        // Calculate broken pipe specific retry delay
        const retryDelay = this.calculateBrokenPipeRetryDelay(message.retry_count + 1);
        const scheduledFor = new Date(Date.now() + retryDelay);

        const retryCount = message.retry_count + 1;
        if (retryCount >= this.maxRetriesBrokenPipe) {
            // Move to DLQ
            await this.moveToDLQ(message, error);
            await this.markMessageFailed(message.id);
            this.metrics.recordDLQEntry(`Broken pipe: ${error.message || String(error)}`);
        } else {
            // Schedule retry with extended delay
            await supabase
                .from('alerts_queue')
                .update({
                    status: 'pending',
                    retry_count: retryCount,
                    scheduled_for: scheduledFor.toISOString(),
                    error: `Broken pipe (attempt ${retryCount}): ${error.message || String(error)}`,
                    locked_by: null,
                    locked_at: null
                })
                .eq('id', message.id);

            logger.warn(`Scheduled broken pipe retry ${retryCount}/${this.maxRetriesBrokenPipe} for message ${message.id} in ${Math.round(retryDelay/1000)}s (extended delay)`);
        }
    }

    private async enterBrokenPipePanicMode(): Promise<void> {
        if (this.brokenPipeState.isInPanicMode) return;

        this.brokenPipeState.isInPanicMode = true;
        this.brokenPipeState.panicModeStartTime = new Date();
        this.brokenPipeState.originalConcurrency = this.concurrency;
        this.brokenPipeState.recoveryAttempts = 0;

        // Emergency concurrency reduction
        this.concurrency = 1;
        
        logger.error({
            originalConcurrency: this.brokenPipeState.originalConcurrency,
            newConcurrency: this.concurrency,
            consecutiveBrokenPipes: this.brokenPipeState.consecutiveBrokenPipes
        }, 'ENTERING BROKEN PIPE PANIC MODE - Emergency concurrency reduction');

        this.metrics.recordConcurrencyChange(
            this.brokenPipeState.originalConcurrency, 
            this.concurrency, 
            'emergency_broken_pipe_reduction'
        );

        // Persist the panic mode state
        await this.persistState();
    }

    private calculateBrokenPipeRetryDelay(retryCount: number): number {
        // Extended exponential backoff for broken pipe: 30s → 90s → 180s
        const baseDelays = [30000, 90000, 180000]; // 30s, 90s, 3min
        const baseDelay = baseDelays[Math.min(retryCount - 1, baseDelays.length - 1)];
        
        // Enhanced jitter: 25-50% to avoid thundering herd
        const jitterMin = 0.25;
        const jitterMax = 0.5;
        const jitter = (jitterMin + Math.random() * (jitterMax - jitterMin)) * baseDelay;
        
        return baseDelay + jitter;
    }

    private async checkBrokenPipeRecovery(): Promise<void> {
        if (!this.brokenPipeState.isInPanicMode) return;

        const timeSincePanic = Date.now() - this.brokenPipeState.panicModeStartTime!.getTime();
        const minPanicDuration = 5 * 60 * 1000; // 5 minutes minimum

        // Don't even consider recovery for first 5 minutes
        if (timeSincePanic < minPanicDuration) {
            logger.debug(`Still in panic mode cooldown (${Math.round((minPanicDuration - timeSincePanic)/1000)}s remaining)`);
            return;
        }

        // Check if we've had recent broken pipes
        const timeSinceLastBrokenPipe = this.brokenPipeState.lastBrokenPipeTime ? 
            Date.now() - this.brokenPipeState.lastBrokenPipeTime.getTime() : Infinity;
        
        const recoveryWindow = 10 * 60 * 1000; // 10 minutes without broken pipes

        if (timeSinceLastBrokenPipe > recoveryWindow) {
            await this.exitBrokenPipePanicMode();
        } else {
            logger.debug(`Waiting for broken pipe recovery (${Math.round((recoveryWindow - timeSinceLastBrokenPipe)/1000)}s remaining)`);
        }
    }

    private async exitBrokenPipePanicMode(): Promise<void> {
        const panicDuration = Date.now() - this.brokenPipeState.panicModeStartTime!.getTime();
        
        logger.info({
            panicDurationMinutes: Math.round(panicDuration / 60000),
            totalBrokenPipes: this.brokenPipeState.brokenPipeCount,
            restoringConcurrency: Math.min(this.brokenPipeState.originalConcurrency, 2) // Conservative restoration
        }, 'EXITING BROKEN PIPE PANIC MODE - Server appears to have recovered');

        // Conservative concurrency restoration - don't go back to full immediately
        const restoredConcurrency = Math.min(this.brokenPipeState.originalConcurrency, 2);
        this.metrics.recordConcurrencyChange(
            this.concurrency, 
            restoredConcurrency, 
            'broken_pipe_panic_recovery'
        );
        
        this.concurrency = restoredConcurrency;
        
        // Reset broken pipe state
        this.brokenPipeState.isInPanicMode = false;
        this.brokenPipeState.consecutiveBrokenPipes = 0;
        this.brokenPipeState.panicModeStartTime = null;
        this.brokenPipeState.recoveryAttempts = 0;

        await this.persistState();
    }

    private onSuccessfulProcessing(): void {
        // Reset consecutive broken pipe counter on any successful processing
        if (this.brokenPipeState.consecutiveBrokenPipes > 0) {
            logger.debug(`Resetting consecutive broken pipe counter from ${this.brokenPipeState.consecutiveBrokenPipes} to 0`);
            this.brokenPipeState.consecutiveBrokenPipes = 0;
        }
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

        // Don't adjust concurrency if we're in broken pipe panic mode
        if (this.brokenPipeState.isInPanicMode) {
            logger.debug('Skipping concurrency adjustment - in broken pipe panic mode')
            // Still reset counters though
            this.successCount = 0
            this.errorCount = 0
            this.lastAdjustment = now
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

    // Sleep mode methods
    private async enterSleepMode(): Promise<void> {
        logger.info('Entering sleep mode - worker stopping, HTTP server staying alive')
        this.isWorkerSleeping = true
        this.updateCircuitBreakerBeforeIdle()
        console.log(`Non-matches: ${JSON.stringify(this.nonMatchedResponses)}`)
        
        // Final state persistence before sleep
        await this.persistState()
        
        logger.info('Worker sleep mode active - HTTP server ready for wake-up calls')
    }

    // Method to wake up consumer (called by edge function)
    public wakeUp(): void {
        if (this.isWorkerSleeping) {
            logger.info('🚀 Consumer woken up by external call - restarting worker')
            this.isWorkerSleeping = false
            this.lastWorkTime = Date.now()
            
            // Reset circuit breaker state when waking up
            this.circuitBreaker.state = 'CLOSED'
            this.circuitBreaker.failures = 0
        } else {
            logger.info('Wake-up call received but worker is already active')
            this.lastWorkTime = Date.now()
        }
    }
}

// Start consumer
const consumer = new QueueConsumer()

// Configuration from environment variables (required)
function getRequiredEnvVar(name: string, defaultValue?: string): string {
    const value = process.env[name] || defaultValue
    if (!value) {
        throw new Error(`Required environment variable ${name} is not set`)
    }
    return value
}

function getOptionalEnvVar(name: string, defaultValue: string): string {
    return process.env[name] || defaultValue
}

const config = {
    serverless: getRequiredEnvVar('CONSUMER_MODE') === 'serverless',
    idleTimeout: parseInt(getOptionalEnvVar('IDLE_TIMEOUT', '120000')),
    enableHttp: getRequiredEnvVar('ENABLE_HTTP') === 'true',
    port: getRequiredEnvVar('PORT')
}

// Apply configuration
consumer['isServerless'] = config.serverless
consumer['idleTimeout'] = config.idleTimeout
consumer['enableHttpServer'] = config.enableHttp

logger.info({ config }, 'Consumer configuration loaded')
logger.info(`🚀 Consumer Mode: ${config.serverless ? 'SERVERLESS' : 'CONTINUOUS'}`)
logger.info(`🌐 HTTP Server: ${config.enableHttp ? 'ENABLED' : 'DISABLED'}`)
logger.info(`⏱️  Idle Timeout: ${config.idleTimeout}ms`)
logger.info(`📡 Port: ${config.port}`)

if (config.enableHttp) {
    logger.info(`🔗 Expected Railway URL: https://sporos-consumer-service-production.up.railway.app`)
    logger.info(`🎯 Test endpoints after deployment:`)
    logger.info(`   curl https://sporos-consumer-service-production.up.railway.app/health`)
    logger.info(`   curl -X POST https://sporos-consumer-service-production.up.railway.app/wake`)
}

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