import pino from 'pino'

const logger = pino()

// Interfaces for structured metrics
export interface ProcessingMetrics {
    totalProcessed: number;
    batchSizes: number[];
    averageBatchSize: number;
    averageProcessingTime: number;
    p95ProcessingTime: number;
    p99ProcessingTime: number;
    langgraphResponseTimes: number[];
}

export interface ThroughputMetrics {
    messagesPerSecond: number;
    messagesPerMinute: number;
    messagesPerHour: number;
    peakThroughput: number;
    throughputTrend: Array<{timestamp: Date, rate: number}>;
}

export interface ResourceMetrics {
    concurrencyUtilization: number;
    concurrencyChanges: Array<{timestamp: Date, old: number, new: number, reason: string}>;
    dbConnectionsActive?: number;
    httpConnectionsActive?: number;
}

export interface MatchQualityMetrics {
    totalMatches: number;
    averageMatchesPerAlert: number;
    noMatchAlerts: number;
    highConfidenceMatches: number;
    matchDistribution: Record<string, number>;
    alertTypeMatchRates: Record<string, number>;
    alertPriorityMatchRates: Record<'low' | 'high', number>;
}

export interface ReliabilityMetrics {
    retryDistribution: Record<number, number>;
    retrySuccessRates: Record<number, number>;
    dlqRate: number;
    dlqReasons: Record<string, number>;
    errorTypes: Record<string, number>;
    timeoutRate: number;
    networkErrorRate: number;
    langgraphErrorRate: number;
    databaseErrorRate: number;
}

export interface CircuitBreakerMetrics {
    stateHistory: Array<{timestamp: Date, state: string, reason: string}>;
    timeInOpen: number;
    timeInHalfOpen: number;
    timeInClosed: number;
    tripFrequency: number;
    recoveryTime: number;
}

export interface AlertMetrics {
    processingTimeP99: number;
    errorRateLast5Min: number;
    successRateLast5Min: number;
    errorRateTrend: 'increasing' | 'decreasing' | 'stable';
    throughputTrend: 'increasing' | 'decreasing' | 'stable';
    anomalies: Array<{
        type: 'error_spike' | 'throughput_drop' | 'latency_spike';
        severity: 'low' | 'medium' | 'high';
        timestamp: Date;
        value: number;
    }>;
}

export interface MetricsSummary {
    performance: ProcessingMetrics;
    throughput: ThroughputMetrics;
    resources: ResourceMetrics;
    quality: MatchQualityMetrics;
    reliability: ReliabilityMetrics;
    circuitBreaker: CircuitBreakerMetrics;
    alerts: AlertMetrics;
    timestamp: Date;
}

// Main metrics collector class
export class MetricsCollector {
    // Processing timing data
    private startTimes = new Map<string, number>();
    private processingTimes: number[] = [];
    private langgraphTimes: number[] = [];
    private batchSizes: number[] = [];
    private matchCounts: number[] = [];
    
    // Error tracking
    private errorsByType: Record<string, number> = {};
    private retrysByAttempt: Record<number, number> = {};
    private retrySuccessByAttempt: Record<number, number> = {};
    private dlqEntries: Array<{reason: string, timestamp: Date}> = [];
    
    // Circuit breaker tracking
    private circuitBreakerEvents: Array<{timestamp: Date, event: string, state: string}> = [];
    private stateStartTimes: Record<string, Date> = {};
    private stateDurations: Record<string, number[]> = {
        OPEN: [],
        HALF_OPEN: [],
        CLOSED: []
    };
    
    // Real-time sliding windows
    private last5MinErrors: Array<{timestamp: Date, error: any, type: string}> = [];
    private last5MinSuccesses: Array<{timestamp: Date}> = [];
    private throughputWindow: Array<{timestamp: Date, count: number}> = [];
    
    // Concurrency tracking
    private concurrencyChanges: Array<{timestamp: Date, old: number, new: number, reason: string}> = [];
    
    // Match quality tracking
    private matchesByAlert: Array<{alertId: string, matchCount: number, alertType?: string, priority?: string}> = [];
    
    // Anomaly detection
    private anomalies: Array<{type: string, severity: string, timestamp: Date, value: number}> = [];
    
    // Configuration
    private maxHistorySize = 1000;
    private cleanupInterval = 5 * 60 * 1000; // 5 minutes
    private lastCleanup = Date.now();

    constructor() {
        // Initialize state tracking
        this.stateStartTimes['CLOSED'] = new Date();
    }

    // === PROCESSING METRICS ===
    
    recordProcessingStart(messageId: string): string {
        const startKey = `${messageId}_${Date.now()}`;
        this.startTimes.set(startKey, Date.now());
        return startKey;
    }

    recordProcessingEnd(startKey: string, success: boolean, matchCount: number = 0, alertId?: string, alertType?: string, priority?: string): void {
        const startTime = this.startTimes.get(startKey);
        if (startTime) {
            const duration = Date.now() - startTime;
            this.processingTimes.push(duration);
            
            if (success) {
                this.last5MinSuccesses.push({timestamp: new Date()});
                this.matchesByAlert.push({
                    alertId: alertId || 'unknown',
                    matchCount,
                    alertType,
                    priority: priority as 'low' | 'high'
                });
                this.matchCounts.push(matchCount);
            }
            
            this.trimArray(this.processingTimes);
            this.trimArray(this.matchCounts);
            this.startTimes.delete(startKey);
        }
        
        this.cleanupOldEntries();
    }

    recordLangGraphTiming(duration: number): void {
        this.langgraphTimes.push(duration);
        this.trimArray(this.langgraphTimes);
    }

    recordBatchSize(size: number): void {
        this.batchSizes.push(size);
        this.throughputWindow.push({timestamp: new Date(), count: size});
        this.trimArray(this.batchSizes);
        this.cleanupOldEntries();
    }

    // === ERROR TRACKING ===
    
    recordError(error: any, context: {alertId?: string, retryCount?: number}): void {
        const errorType = this.classifyError(error);
        this.errorsByType[errorType] = (this.errorsByType[errorType] || 0) + 1;
        
        if (context.retryCount !== undefined) {
            this.retrysByAttempt[context.retryCount] = (this.retrysByAttempt[context.retryCount] || 0) + 1;
        }
        
        this.last5MinErrors.push({
            timestamp: new Date(),
            error,
            type: errorType
        });
        
        // Check for error rate spike
        this.checkForAnomalies();
        this.cleanupOldEntries();
    }

    recordRetrySuccess(retryAttempt: number): void {
        this.retrySuccessByAttempt[retryAttempt] = (this.retrySuccessByAttempt[retryAttempt] || 0) + 1;
    }

    recordDLQEntry(reason: string): void {
        this.dlqEntries.push({reason, timestamp: new Date()});
        this.dlqReasons[reason] = (this.dlqReasons[reason] || 0) + 1;
    }

    // === CIRCUIT BREAKER TRACKING ===
    
    recordCircuitBreakerEvent(event: string, newState: string, oldState?: string): void {
        const now = new Date();
        
        this.circuitBreakerEvents.push({
            timestamp: now,
            event,
            state: newState
        });
        
        // Track state duration
        if (oldState && this.stateStartTimes[oldState]) {
            const duration = now.getTime() - this.stateStartTimes[oldState].getTime();
            this.stateDurations[oldState].push(duration);
        }
        
        this.stateStartTimes[newState] = now;
        
        // Trim events history
        if (this.circuitBreakerEvents.length > 100) {
            this.circuitBreakerEvents.shift();
        }
    }

    // === CONCURRENCY TRACKING ===
    
    recordConcurrencyChange(oldLevel: number, newLevel: number, reason: string): void {
        this.concurrencyChanges.push({
            timestamp: new Date(),
            old: oldLevel,
            new: newLevel,
            reason
        });
        
        // Keep last 100 changes
        if (this.concurrencyChanges.length > 100) {
            this.concurrencyChanges.shift();
        }
    }

    // === UTILITY METHODS ===
    
    private classifyError(error: any): string {
        if (error.name === 'AbortError') return 'timeout';
        if (error.message?.includes('LangGraph error: 5')) return 'langgraph_server_error';
        if (error.message?.includes('LangGraph error: 4')) return 'langgraph_client_error';
        if (error.message?.includes('database') || error.message?.includes('supabase')) return 'database_error';
        if (error.message?.includes('network') || error.message?.includes('fetch')) return 'network_error';
        if (error.message?.includes('timeout')) return 'timeout';
        return 'unknown_error';
    }

    private trimArray(arr: number[]): void {
        if (arr.length > this.maxHistorySize) {
            arr.splice(0, arr.length - this.maxHistorySize);
        }
    }

    private cleanupOldEntries(): void {
        if (Date.now() - this.lastCleanup < this.cleanupInterval) return;
        
        const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
        const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
        
        this.last5MinErrors = this.last5MinErrors.filter(e => e.timestamp > fiveMinutesAgo);
        this.last5MinSuccesses = this.last5MinSuccesses.filter(s => s.timestamp > fiveMinutesAgo);
        this.throughputWindow = this.throughputWindow.filter(t => t.timestamp > fiveMinutesAgo);
        this.dlqEntries = this.dlqEntries.filter(d => d.timestamp > oneHourAgo);
        this.matchesByAlert = this.matchesByAlert.filter(m => Date.now() - Date.parse(m.alertId) < 24 * 60 * 60 * 1000); // Keep 24h
        
        this.lastCleanup = Date.now();
    }

    private checkForAnomalies(): void {
        const errorRate = this.getErrorRateLast5Min();
        const throughput = this.getCurrentThroughput();
        
        // Error spike detection
        if (errorRate > 0.2) {
            this.anomalies.push({
                type: 'error_spike',
                severity: errorRate > 0.5 ? 'high' : 'medium',
                timestamp: new Date(),
                value: errorRate
            });
        }
        
        // Throughput drop detection (if we have historical data)
        if (this.throughputWindow.length > 10 && throughput < this.getAverageThroughput() * 0.5) {
            this.anomalies.push({
                type: 'throughput_drop',
                severity: throughput < this.getAverageThroughput() * 0.2 ? 'high' : 'medium',
                timestamp: new Date(),
                value: throughput
            });
        }
        
        // Keep only recent anomalies
        const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
        this.anomalies = this.anomalies.filter(a => a.timestamp > oneHourAgo);
    }

    // === CALCULATION METHODS ===
    
    private average(arr: number[]): number {
        return arr.length === 0 ? 0 : arr.reduce((sum, val) => sum + val, 0) / arr.length;
    }

    private percentile(arr: number[], p: number): number {
        if (arr.length === 0) return 0;
        const sorted = [...arr].sort((a, b) => a - b);
        const index = Math.ceil((p / 100) * sorted.length) - 1;
        return sorted[Math.max(0, index)];
    }

    private getErrorRateLast5Min(): number {
        this.cleanupOldEntries();
        const totalEvents = this.last5MinErrors.length + this.last5MinSuccesses.length;
        return totalEvents === 0 ? 0 : this.last5MinErrors.length / totalEvents;
    }

    private getSuccessRateLast5Min(): number {
        return 1 - this.getErrorRateLast5Min();
    }

    private getCurrentThroughput(): number {
        this.cleanupOldEntries();
        const totalMessages = this.throughputWindow.reduce((sum, t) => sum + t.count, 0);
        return totalMessages / 5; // messages per minute (over 5 minute window)
    }

    private getAverageThroughput(): number {
        if (this.throughputWindow.length === 0) return 0;
        return this.average(this.throughputWindow.map(t => t.count));
    }

    private getTrendDirection(values: number[]): 'increasing' | 'decreasing' | 'stable' {
        if (values.length < 3) return 'stable';
        
        const recent = values.slice(-3);
        const isIncreasing = recent[2] > recent[1] && recent[1] > recent[0];
        const isDecreasing = recent[2] < recent[1] && recent[1] < recent[0];
        
        if (isIncreasing) return 'increasing';
        if (isDecreasing) return 'decreasing';
        return 'stable';
    }

    // === PUBLIC GETTERS ===
    
    private dlqReasons: Record<string, number> = {};

    getMetricsSummary(): MetricsSummary {
        this.cleanupOldEntries();
        
        // Calculate match distribution
        const matchDistribution: Record<string, number> = {};
        this.matchCounts.forEach(count => {
            const key = count === 0 ? 'no_matches' : count === 1 ? 'single_match' : 'multiple_matches';
            matchDistribution[key] = (matchDistribution[key] || 0) + 1;
        });

        // Calculate alert type match rates
        const alertTypeMatchRates: Record<string, number> = {};
        const alertPriorityMatchRates: Record<'low' | 'high', number> = { low: 0, high: 0 };
        
        this.matchesByAlert.forEach(match => {
            if (match.alertType) {
                if (!alertTypeMatchRates[match.alertType]) {
                    alertTypeMatchRates[match.alertType] = 0;
                }
                if (match.matchCount > 0) {
                    alertTypeMatchRates[match.alertType]++;
                }
            }
            
            if (match.priority) {
                if (match.matchCount > 0) {
                    alertPriorityMatchRates[match.priority]++;
                }
            }
        });

        // Calculate retry success rates
        const retrySuccessRates: Record<number, number> = {};
        Object.keys(this.retrysByAttempt).forEach(attempt => {
            const attemptNum = parseInt(attempt);
            const totalRetries = this.retrysByAttempt[attemptNum] || 0;
            const successfulRetries = this.retrySuccessByAttempt[attemptNum] || 0;
            retrySuccessRates[attemptNum] = totalRetries === 0 ? 0 : successfulRetries / totalRetries;
        });

        return {
            performance: {
                totalProcessed: this.processingTimes.length,
                batchSizes: [...this.batchSizes],
                averageBatchSize: this.average(this.batchSizes),
                averageProcessingTime: this.average(this.processingTimes),
                p95ProcessingTime: this.percentile(this.processingTimes, 95),
                p99ProcessingTime: this.percentile(this.processingTimes, 99),
                langgraphResponseTimes: [...this.langgraphTimes]
            },
            
            throughput: {
                messagesPerSecond: this.getCurrentThroughput() / 60,
                messagesPerMinute: this.getCurrentThroughput(),
                messagesPerHour: this.getCurrentThroughput() * 60,
                peakThroughput: Math.max(...this.throughputWindow.map(t => t.count)),
                throughputTrend: this.throughputWindow.slice(-10).map(t => ({
                    timestamp: t.timestamp,
                    rate: t.count
                }))
            },
            
            resources: {
                concurrencyUtilization: 0, // Will be set by consumer
                concurrencyChanges: [...this.concurrencyChanges.slice(-10)]
            },
            
            quality: {
                totalMatches: this.matchCounts.reduce((sum, count) => sum + count, 0),
                averageMatchesPerAlert: this.average(this.matchCounts),
                noMatchAlerts: this.matchCounts.filter(c => c === 0).length,
                highConfidenceMatches: this.matchCounts.filter(c => c > 0).length,
                matchDistribution,
                alertTypeMatchRates,
                alertPriorityMatchRates
            },
            
            reliability: {
                retryDistribution: {...this.retrysByAttempt},
                retrySuccessRates,
                dlqRate: this.dlqEntries.length / Math.max(this.processingTimes.length, 1),
                dlqReasons: {...this.dlqReasons},
                errorTypes: {...this.errorsByType},
                timeoutRate: (this.errorsByType['timeout'] || 0) / Math.max(this.processingTimes.length, 1),
                networkErrorRate: (this.errorsByType['network_error'] || 0) / Math.max(this.processingTimes.length, 1),
                langgraphErrorRate: ((this.errorsByType['langgraph_server_error'] || 0) + (this.errorsByType['langgraph_client_error'] || 0)) / Math.max(this.processingTimes.length, 1),
                databaseErrorRate: (this.errorsByType['database_error'] || 0) / Math.max(this.processingTimes.length, 1)
            },
            
            circuitBreaker: {
                stateHistory: [...this.circuitBreakerEvents.slice(-10).map(e => ({
                    timestamp: e.timestamp,
                    state: e.state,
                    reason: e.event
                }))],
                timeInOpen: this.average(this.stateDurations['OPEN'] || []),
                timeInHalfOpen: this.average(this.stateDurations['HALF_OPEN'] || []),
                timeInClosed: this.average(this.stateDurations['CLOSED'] || []),
                tripFrequency: this.circuitBreakerEvents.filter(e => e.event.includes('opened')).length,
                recoveryTime: this.average(this.stateDurations['OPEN'] || [])
            },
            
            alerts: {
                processingTimeP99: this.percentile(this.processingTimes, 99),
                errorRateLast5Min: this.getErrorRateLast5Min(),
                successRateLast5Min: this.getSuccessRateLast5Min(),
                errorRateTrend: this.getTrendDirection(this.last5MinErrors.map(() => 1)),
                throughputTrend: this.getTrendDirection(this.throughputWindow.map(t => t.count)),
                anomalies: [...this.anomalies.slice(-5).map(a => ({
                    type: a.type as 'error_spike' | 'throughput_drop' | 'latency_spike',
                    severity: a.severity as 'low' | 'medium' | 'high',
                    timestamp: a.timestamp,
                    value: a.value
                }))]
            },
            
            timestamp: new Date()
        };
    }

    getSimpleMetrics() {
        return {
            processedCount: this.processingTimes.length,
            averageProcessingTime: this.average(this.processingTimes),
            errorRate: this.getErrorRateLast5Min(),
            successRate: this.getSuccessRateLast5Min(),
            currentThroughput: this.getCurrentThroughput(),
            averageMatches: this.average(this.matchCounts),
            noMatchAlerts: this.matchCounts.filter(c => c === 0).length,
            errorsByType: {...this.errorsByType},
            recentAnomalies: this.anomalies.slice(-3)
        };
    }

    // Export methods for external monitoring
    exportMetricsForPrometheus(): string {
        const metrics = this.getSimpleMetrics();
        return [
            `# HELP consumer_processed_total Total number of processed messages`,
            `# TYPE consumer_processed_total counter`,
            `consumer_processed_total ${metrics.processedCount}`,
            ``,
            `# HELP consumer_processing_duration_ms Average processing time in milliseconds`,
            `# TYPE consumer_processing_duration_ms gauge`,
            `consumer_processing_duration_ms ${metrics.averageProcessingTime}`,
            ``,
            `# HELP consumer_error_rate Current error rate`,
            `# TYPE consumer_error_rate gauge`,
            `consumer_error_rate ${metrics.errorRate}`,
            ``,
            `# HELP consumer_throughput_per_minute Messages processed per minute`,
            `# TYPE consumer_throughput_per_minute gauge`,
            `consumer_throughput_per_minute ${metrics.currentThroughput}`
        ].join('\n');
    }

    exportMetricsForInfluxDB(): Array<{measurement: string, tags: Record<string, string>, fields: Record<string, number>, timestamp: number}> {
        const metrics = this.getSimpleMetrics();
        const timestamp = Date.now() * 1000000; // InfluxDB expects nanoseconds
        
        return [
            {
                measurement: 'consumer_performance',
                tags: {service: 'alert-consumer'},
                fields: {
                    processed_count: metrics.processedCount,
                    avg_processing_time: metrics.averageProcessingTime,
                    error_rate: metrics.errorRate,
                    success_rate: metrics.successRate,
                    throughput: metrics.currentThroughput
                },
                timestamp
            },
            {
                measurement: 'consumer_quality',
                tags: {service: 'alert-consumer'},
                fields: {
                    avg_matches: metrics.averageMatches,
                    no_match_alerts: metrics.noMatchAlerts
                },
                timestamp
            }
        ];
    }

    // Reset methods for testing
    reset(): void {
        this.startTimes.clear();
        this.processingTimes = [];
        this.langgraphTimes = [];
        this.batchSizes = [];
        this.matchCounts = [];
        this.errorsByType = {};
        this.retrysByAttempt = {};
        this.retrySuccessByAttempt = {};
        this.dlqEntries = [];
        this.circuitBreakerEvents = [];
        this.last5MinErrors = [];
        this.last5MinSuccesses = [];
        this.throughputWindow = [];
        this.concurrencyChanges = [];
        this.matchesByAlert = [];
        this.anomalies = [];
        this.dlqReasons = {};
    }

    // Logging helper
    logMetricsSummary(): void {
        const summary = this.getSimpleMetrics();
        logger.info({
            metrics: {
                processed: summary.processedCount,
                avgProcessingTime: `${summary.averageProcessingTime.toFixed(2)}ms`,
                errorRate: `${(summary.errorRate * 100).toFixed(2)}%`,
                throughput: `${summary.currentThroughput.toFixed(2)}/min`,
                avgMatches: summary.averageMatches.toFixed(2),
                noMatchAlerts: summary.noMatchAlerts,
                errors: summary.errorsByType,
                anomalies: summary.recentAnomalies.length
            }
        }, 'Metrics Summary');
    }
}

// Helper functions for easy integration
export function createMetricsCollector(): MetricsCollector {
    return new MetricsCollector();
}

export function formatMetricsForDashboard(metrics: MetricsSummary): Record<string, any> {
    return {
        overview: {
            totalProcessed: metrics.performance.totalProcessed,
            currentThroughput: `${metrics.throughput.messagesPerMinute.toFixed(1)}/min`,
            errorRate: `${(metrics.alerts.errorRateLast5Min * 100).toFixed(2)}%`,
            avgProcessingTime: `${metrics.performance.averageProcessingTime.toFixed(0)}ms`
        },
        quality: {
            averageMatches: metrics.quality.averageMatchesPerAlert.toFixed(2),
            noMatchRate: `${((metrics.quality.noMatchAlerts / Math.max(metrics.performance.totalProcessed, 1)) * 100).toFixed(1)}%`,
            totalMatches: metrics.quality.totalMatches
        },
        reliability: {
            dlqRate: `${(metrics.reliability.dlqRate * 100).toFixed(2)}%`,
            timeoutRate: `${(metrics.reliability.timeoutRate * 100).toFixed(2)}%`,
            retrySuccessRate: Object.values(metrics.reliability.retrySuccessRates).length > 0 
                ? `${(Object.values(metrics.reliability.retrySuccessRates).reduce((a, b) => a + b, 0) / Object.values(metrics.reliability.retrySuccessRates).length * 100).toFixed(1)}%`
                : '0%'
        },
        alerts: metrics.alerts.anomalies.length > 0 ? {
            recentAnomalies: metrics.alerts.anomalies.map(a => ({
                type: a.type.replace('_', ' '),
                severity: a.severity,
                time: a.timestamp.toISOString()
            }))
        } : null
    };
}
