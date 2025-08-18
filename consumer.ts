import pino from 'pino'
import 'dotenv/config'

import { createClient } from '@supabase/supabase-js'

const logger = pino()
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  logger.error('Environment variables SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY are missing or inaccessible.');
} else {
  logger.info('Environment variables loaded successfully.');
}

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

/**
 * 
 * We'll have:
 * - Adaptive Concurrency method
 * - Circuit Breaker with db persistence
 * - Heartbeat to keep the consumer alive
 * - Jitter for retry delays
 * - DLQ for failed messages
 */

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
    private maxConcurrency = 3;
    private consumerId = `consumer-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    private successCount = 0;
    private errorCount = 0;
    private lastAdjustment = Date.now();
    private adjustmentInterval = 30000; // 30 seconds
    private successThreshold = 0.95; // 95% success rate to increase concurrency
    private maxRetries = 3;
    
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
        logger.info(`Consumer starting with ID: ${this.consumerId}`)

        // Restore state from DB
        await this.restoreState() 
        
        // Main processing loop
        while (true) {
            try {
                if (this.canProcess()) {
                    await this.processNextBatch()
                } else {
                    logger.info('Circuit breaker OPEN, waiting...')
                    await this.sleep(this.circuitBreaker.timeout / 4) // Check every quarter timeout period
                }
                
                await this.sendHeartbeat()
                await this.adjustConcurrency()
            } catch (error) {
                logger.error({ error }, 'Processing error')
                await this.handleError(error)
            }

            // Small delay between batches
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
    private async processNextBatch(): Promise<void> {
        const messages = await this.fetchNextBatch()
        if (messages.length === 0) {
            await this.sleep(2000) // No messages, wait longer
            return
        }

        logger.info(`Processing batch of ${messages.length} messages with concurrency ${this.concurrency}`)

        // Process messages with limited concurrency
        const promises = messages.map(message => this.processMessage(message))
        await Promise.allSettled(promises)
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
        try {
            logger.info(`Processing message ${message.id} for alert ${message.alert_id}`)

            // Simulate processing - replace with actual alert processing logic
            await this.processAlert(message.payload)

            // Mark as completed
            await this.markMessageCompleted(message.id)
            this.successCount++
            
            // Update circuit breaker on success
            if (this.circuitBreaker.state === 'HALF_OPEN') {
                this.circuitBreaker.halfOpenCalls++
                if (this.circuitBreaker.halfOpenCalls >= this.circuitBreaker.halfOpenMaxCalls) {
                    this.circuitBreaker.state = 'CLOSED'
                    this.circuitBreaker.failures = 0
                    this.circuitBreaker.halfOpenCalls = 0
                    logger.info('Circuit breaker closed after successful half-open calls')
                }
            }

        } catch (error) {
            logger.error({ error, messageId: message.id }, 'Failed to process message')
            await this.handleMessageError(message, error)
            this.errorCount++
            this.updateCircuitBreakerOnFailure()
        }
    }

    private async processAlert(payload: AlertPayload): Promise<void> {
        // TODO: Implement actual alert processing logic
        
        // Simulate processing time
        await this.sleep(100 + Math.random() * 200)
        logger.info(`Processed alert ${payload.id}`)
        
        // Simulate occasional failures for testing
        if (Math.random() < 0.5) { // 50% failure rate
            throw new Error('Simulated processing failure')
        }
    }

    private async markMessageCompleted(messageId: string): Promise<void> {
        await supabase
            .from('alerts_queue')
            .update({
                status: 'completed',
                completed_at: new Date().toISOString(),
                locked_by: null,
                locked_at: null
            })
            .eq('id', messageId)
    }

    private async handleMessageError(message: AlertsQueue, error: any): Promise<void> {
        const retryCount = message.retry_count + 1

        if (retryCount >= this.maxRetries) {
            // Move to DLQ
            await this.moveToDLQ(message, error)
            await this.markMessageFailed(message.id)
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
        this.circuitBreaker.failures++
        this.circuitBreaker.lastFailure = new Date()

        if (this.circuitBreaker.failures >= this.circuitBreaker.threshold) {
            this.circuitBreaker.state = 'OPEN'
            this.circuitBreaker.halfOpenCalls = 0
            logger.warn(`Circuit breaker opened after ${this.circuitBreaker.failures} failures`)
        }
    }

    private async sendHeartbeat(): Promise<void> {
        try {
            await this.persistState()
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

        if (successRate >= this.successThreshold && this.concurrency < this.maxConcurrency) {
            // Increase concurrency
            this.concurrency = Math.min(this.concurrency + 1, this.maxConcurrency)
            logger.info(`Increased concurrency from ${currentConcurrency} to ${this.concurrency} (success rate: ${(successRate * 100).toFixed(2)}%)`)
        } else if (successRate < 0.8 && this.concurrency > this.minConcurrency) {
            // Decrease concurrency
            this.concurrency = Math.max(this.concurrency - 1, this.minConcurrency)
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
}

// Start consumer
const consumer = new QueueConsumer()
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