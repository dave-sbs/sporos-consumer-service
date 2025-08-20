"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MetricsCollector = void 0;
exports.createMetricsCollector = createMetricsCollector;
exports.formatMetricsForDashboard = formatMetricsForDashboard;
const pino_1 = __importDefault(require("pino"));
const logger = (0, pino_1.default)();
// Main metrics collector class
class MetricsCollector {
    constructor() {
        // Processing timing data
        this.startTimes = new Map();
        this.processingTimes = [];
        this.langgraphTimes = [];
        this.batchSizes = [];
        this.matchCounts = [];
        // Error tracking
        this.errorsByType = {};
        this.retrysByAttempt = {};
        this.retrySuccessByAttempt = {};
        this.dlqEntries = [];
        // Circuit breaker tracking
        this.circuitBreakerEvents = [];
        this.stateStartTimes = {};
        this.stateDurations = {
            OPEN: [],
            HALF_OPEN: [],
            CLOSED: []
        };
        // Real-time sliding windows
        this.last5MinErrors = [];
        this.last5MinSuccesses = [];
        this.throughputWindow = [];
        // Concurrency tracking
        this.concurrencyChanges = [];
        // Match quality tracking
        this.matchesByAlert = [];
        // Anomaly detection
        this.anomalies = [];
        // Configuration
        this.maxHistorySize = 1000;
        this.cleanupInterval = 5 * 60 * 1000; // 5 minutes
        this.lastCleanup = Date.now();
        // === PUBLIC GETTERS ===
        this.dlqReasons = {};
        // Initialize state tracking
        this.stateStartTimes['CLOSED'] = new Date();
    }
    // === PROCESSING METRICS ===
    recordProcessingStart(messageId) {
        const startKey = `${messageId}_${Date.now()}`;
        this.startTimes.set(startKey, Date.now());
        return startKey;
    }
    recordProcessingEnd(startKey, success, matchCount = 0, alertId, alertType, priority) {
        const startTime = this.startTimes.get(startKey);
        if (startTime) {
            const duration = Date.now() - startTime;
            this.processingTimes.push(duration);
            if (success) {
                this.last5MinSuccesses.push({ timestamp: new Date() });
                this.matchesByAlert.push({
                    alertId: alertId || 'unknown',
                    matchCount,
                    alertType,
                    priority: priority
                });
                this.matchCounts.push(matchCount);
            }
            this.trimArray(this.processingTimes);
            this.trimArray(this.matchCounts);
            this.startTimes.delete(startKey);
        }
        this.cleanupOldEntries();
    }
    recordLangGraphTiming(duration) {
        this.langgraphTimes.push(duration);
        this.trimArray(this.langgraphTimes);
    }
    recordBatchSize(size) {
        this.batchSizes.push(size);
        this.throughputWindow.push({ timestamp: new Date(), count: size });
        this.trimArray(this.batchSizes);
        this.cleanupOldEntries();
    }
    // === ERROR TRACKING ===
    recordError(error, context) {
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
    recordRetrySuccess(retryAttempt) {
        this.retrySuccessByAttempt[retryAttempt] = (this.retrySuccessByAttempt[retryAttempt] || 0) + 1;
    }
    recordDLQEntry(reason) {
        this.dlqEntries.push({ reason, timestamp: new Date() });
        this.dlqReasons[reason] = (this.dlqReasons[reason] || 0) + 1;
    }
    // === CIRCUIT BREAKER TRACKING ===
    recordCircuitBreakerEvent(event, newState, oldState) {
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
    recordConcurrencyChange(oldLevel, newLevel, reason) {
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
    classifyError(error) {
        if (error.name === 'AbortError')
            return 'timeout';
        if (error.message?.includes('LangGraph error: 5'))
            return 'langgraph_server_error';
        if (error.message?.includes('LangGraph error: 4'))
            return 'langgraph_client_error';
        if (error.message?.includes('database') || error.message?.includes('supabase'))
            return 'database_error';
        if (error.message?.includes('network') || error.message?.includes('fetch'))
            return 'network_error';
        if (error.message?.includes('timeout'))
            return 'timeout';
        return 'unknown_error';
    }
    trimArray(arr) {
        if (arr.length > this.maxHistorySize) {
            arr.splice(0, arr.length - this.maxHistorySize);
        }
    }
    cleanupOldEntries() {
        if (Date.now() - this.lastCleanup < this.cleanupInterval)
            return;
        const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
        const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
        this.last5MinErrors = this.last5MinErrors.filter(e => e.timestamp > fiveMinutesAgo);
        this.last5MinSuccesses = this.last5MinSuccesses.filter(s => s.timestamp > fiveMinutesAgo);
        this.throughputWindow = this.throughputWindow.filter(t => t.timestamp > fiveMinutesAgo);
        this.dlqEntries = this.dlqEntries.filter(d => d.timestamp > oneHourAgo);
        this.matchesByAlert = this.matchesByAlert.filter(m => Date.now() - Date.parse(m.alertId) < 24 * 60 * 60 * 1000); // Keep 24h
        this.lastCleanup = Date.now();
    }
    checkForAnomalies() {
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
    average(arr) {
        return arr.length === 0 ? 0 : arr.reduce((sum, val) => sum + val, 0) / arr.length;
    }
    percentile(arr, p) {
        if (arr.length === 0)
            return 0;
        const sorted = [...arr].sort((a, b) => a - b);
        const index = Math.ceil((p / 100) * sorted.length) - 1;
        return sorted[Math.max(0, index)];
    }
    getErrorRateLast5Min() {
        this.cleanupOldEntries();
        const totalEvents = this.last5MinErrors.length + this.last5MinSuccesses.length;
        return totalEvents === 0 ? 0 : this.last5MinErrors.length / totalEvents;
    }
    getSuccessRateLast5Min() {
        return 1 - this.getErrorRateLast5Min();
    }
    getCurrentThroughput() {
        this.cleanupOldEntries();
        const totalMessages = this.throughputWindow.reduce((sum, t) => sum + t.count, 0);
        return totalMessages / 5; // messages per minute (over 5 minute window)
    }
    getAverageThroughput() {
        if (this.throughputWindow.length === 0)
            return 0;
        return this.average(this.throughputWindow.map(t => t.count));
    }
    getTrendDirection(values) {
        if (values.length < 3)
            return 'stable';
        const recent = values.slice(-3);
        const isIncreasing = recent[2] > recent[1] && recent[1] > recent[0];
        const isDecreasing = recent[2] < recent[1] && recent[1] < recent[0];
        if (isIncreasing)
            return 'increasing';
        if (isDecreasing)
            return 'decreasing';
        return 'stable';
    }
    getMetricsSummary() {
        this.cleanupOldEntries();
        // Calculate match distribution
        const matchDistribution = {};
        this.matchCounts.forEach(count => {
            const key = count === 0 ? 'no_matches' : count === 1 ? 'single_match' : 'multiple_matches';
            matchDistribution[key] = (matchDistribution[key] || 0) + 1;
        });
        // Calculate alert type match rates
        const alertTypeMatchRates = {};
        const alertPriorityMatchRates = { low: 0, high: 0 };
        this.matchesByAlert.forEach(match => {
            if (match.alertType) {
                if (!alertTypeMatchRates[match.alertType]) {
                    alertTypeMatchRates[match.alertType] = 0;
                }
                if (match.matchCount > 0) {
                    alertTypeMatchRates[match.alertType]++;
                }
            }
            if (match.priority && (match.priority === 'low' || match.priority === 'high')) {
                if (match.matchCount > 0) {
                    alertPriorityMatchRates[match.priority]++;
                }
            }
        });
        // Calculate retry success rates
        const retrySuccessRates = {};
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
                retryDistribution: { ...this.retrysByAttempt },
                retrySuccessRates,
                dlqRate: this.dlqEntries.length / Math.max(this.processingTimes.length, 1),
                dlqReasons: { ...this.dlqReasons },
                errorTypes: { ...this.errorsByType },
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
                        type: a.type,
                        severity: a.severity,
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
            errorsByType: { ...this.errorsByType },
            recentAnomalies: this.anomalies.slice(-3)
        };
    }
    // Export methods for external monitoring
    exportMetricsForPrometheus() {
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
    exportMetricsForInfluxDB() {
        const metrics = this.getSimpleMetrics();
        const timestamp = Date.now() * 1000000; // InfluxDB expects nanoseconds
        return [
            {
                measurement: 'consumer_performance',
                tags: { service: 'alert-consumer' },
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
                tags: { service: 'alert-consumer' },
                fields: {
                    avg_matches: metrics.averageMatches,
                    no_match_alerts: metrics.noMatchAlerts
                },
                timestamp
            }
        ];
    }
    // Reset methods for testing
    reset() {
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
    logMetricsSummary() {
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
exports.MetricsCollector = MetricsCollector;
// Helper functions for easy integration
function createMetricsCollector() {
    return new MetricsCollector();
}
function formatMetricsForDashboard(metrics) {
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
