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
    private concurrency = 1; //increment by 1 when testing batch uploads and success threshold is met
    private circuitBreaker = {
        state: 'CLOSED' as 'CLOSED' | 'OPEN' | 'HALF_OPEN',
        failures: 0,
        lastFailure: null as Date | null,
        threshold: 5,
        timeout: 60000 // 1 minute
    }

    async start() {
        logger.info('Consumer starting...')

        // Restore state from DB
        await this.restoreState() 
        
        // Main processing loop
        while (true) {
            try {
                await this.processNextBatch()
                await this.sendHeartbeat()
            } catch (error) {
                logger.error({ error }, 'Processing error')
                await this.handleError(error)
            }

            // Small delay between batches
            await this.sleep(1000)
        }
    }

    // DB Persistence Methods
    private async restoreState(): Promise<QueueConsumerState | Error> {
        const { data, error } = await supabase.from('alerts_queue_consumer_state').select('*').single()
        if (error) {
            logger.error({ error }, 'Error fetching consumer state')
            throw new Error('Error fetching consumer state')
        }

        return data as QueueConsumerState
    }

    private async persistState(state: QueueConsumerState): Promise<void> {
        await supabase
            .from('alerts_queue_consumer_state')
            .upsert(state)
    }

    // Processing Methods
    private async processNextBatch() {
        // Check circuit breaker
        if (!this.canProcess()) {
            logger.info('Circuit breaker OPEN, waiting...')
            // Sleep for some time - determined by db or what?
            return
        }
    }

    private async sendHeartbeat() {
    }

    private async handleError(error: any) {
    }

    // Utility Methods
    private async sleep(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms))
    }

    private canProcess(): boolean {
        if (this.circuitBreaker.state === 'CLOSED') return true

        // If circuit is OPEN, check if enought time has passed and set to HALF_OPEN
        if (this.circuitBreaker.state === 'OPEN') {
            const timeSinceFailure = Date.now() - this.circuitBreaker.lastFailure!.getTime()
            if (timeSinceFailure > this.circuitBreaker.timeout) {
                this.circuitBreaker.state = 'HALF_OPEN'
                logger.info('Circuit breaker half-open, trying again...')
                return true
            }
            return false // Still OPEN
        }

        return true // HALF_OPEN
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
    logger.info('Shutting down gracefully...')
})