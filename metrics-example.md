# Metrics System Usage Guide

## Overview

The metrics system has been integrated into your `QueueConsumer` class and provides comprehensive monitoring of your alert processing pipeline.

## Available Endpoints

When you enable the HTTP server (`ENABLE_HTTP=true`), you get these metrics endpoints:

### 1. Dashboard Metrics - `/metrics`
```bash
curl http://localhost:3000/metrics
```
Returns a formatted, user-friendly view:
```json
{
  "overview": {
    "totalProcessed": 245,
    "currentThroughput": "12.3/min",
    "errorRate": "2.45%",
    "avgProcessingTime": "1234ms"
  },
  "quality": {
    "averageMatches": "2.4",
    "noMatchRate": "15.2%",
    "totalMatches": 589
  },
  "reliability": {
    "dlqRate": "0.8%",
    "timeoutRate": "1.2%",
    "retrySuccessRate": "78.5%"
  },
  "alerts": {
    "recentAnomalies": [
      {
        "type": "error spike",
        "severity": "medium",
        "time": "2024-01-15T10:30:00.000Z"
      }
    ]
  }
}
```

### 2. Raw Metrics - `/metrics/raw`
```bash
curl http://localhost:3000/metrics/raw
```
Returns the complete metrics object with all detailed data.

### 3. Prometheus Format - `/metrics/prometheus`
```bash
curl http://localhost:3000/metrics/prometheus
```
Returns metrics in Prometheus format for monitoring systems:
```
# HELP consumer_processed_total Total number of processed messages
# TYPE consumer_processed_total counter
consumer_processed_total 245

# HELP consumer_processing_duration_ms Average processing time in milliseconds
# TYPE consumer_processing_duration_ms gauge
consumer_processing_duration_ms 1234
```

## Key Metrics Tracked

### Performance Metrics
- **Processing Times**: Average, P95, P99 processing durations
- **LangGraph Response Times**: API call latencies to LangGraph
- **Batch Sizes**: Distribution of batch processing volumes
- **Throughput**: Messages per second/minute/hour

### Quality Metrics
- **Match Rates**: How many alerts get matches vs no matches
- **Match Distribution**: Single match vs multiple matches
- **Alert Type Performance**: Match rates by alert type/priority
- **Average Matches per Alert**: Quality indicator

### Reliability Metrics
- **Error Types**: Breakdown by timeout, network, API, database errors
- **Retry Patterns**: Success rates by retry attempt (1st, 2nd, 3rd)
- **DLQ Rates**: How often messages end up in dead letter queue
- **Circuit Breaker**: State changes and recovery patterns

### System Health
- **Concurrency Changes**: When and why concurrency is adjusted
- **Circuit Breaker Events**: Opens, closes, half-open states
- **Anomaly Detection**: Error spikes, throughput drops
- **Resource Utilization**: Current vs max concurrency

## Environment Variables

To enable metrics endpoints:
```bash
ENABLE_HTTP=true
PORT=3000
```

## Logging

The system automatically logs a metrics summary every 5 minutes:
```
INFO: Metrics Summary {
  "metrics": {
    "processed": 125,
    "avgProcessingTime": "1.2s",
    "errorRate": "2.1%",
    "throughput": "8.5/min",
    "avgMatches": 1.8,
    "noMatchAlerts": 12,
    "errors": {
      "timeout": 3,
      "langgraph_server_error": 1
    },
    "anomalies": 0
  }
}
```

## Integration with Monitoring Systems

### For Grafana/Prometheus
Use the `/metrics/prometheus` endpoint and configure Prometheus to scrape:
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'alert-consumer'
    static_configs:
      - targets: ['localhost:3000']
    metrics_path: '/metrics/prometheus'
```

### For InfluxDB
The metrics system includes an export method:
```typescript
const influxData = consumer.metrics.exportMetricsForInfluxDB()
// Send to InfluxDB
```

### For Custom Dashboards
Use the `/metrics` endpoint for easy dashboard integration:
```javascript
// Frontend dashboard
const response = await fetch('/metrics')
const metrics = await response.json()
// Display metrics.overview, metrics.quality, etc.
```

## Alerts and Thresholds

The system automatically detects anomalies:
- **Error Rate > 20%**: Medium severity alert
- **Error Rate > 50%**: High severity alert
- **Throughput drops > 50%**: Medium severity
- **Throughput drops > 80%**: High severity

## Example Usage Scenarios

### 1. Debugging Performance Issues
```bash
# Check current performance
curl localhost:3000/metrics | jq '.overview'

# Look for error patterns
curl localhost:3000/metrics/raw | jq '.reliability.errorTypes'

# Check LangGraph response times
curl localhost:3000/metrics/raw | jq '.performance.langgraphResponseTimes'
```

### 2. Quality Analysis
```bash
# Check match quality
curl localhost:3000/metrics | jq '.quality'

# See which alert types perform better
curl localhost:3000/metrics/raw | jq '.quality.alertTypeMatchRates'
```

### 3. System Health Monitoring
```bash
# Check for recent anomalies
curl localhost:3000/metrics | jq '.alerts'

# Monitor circuit breaker health
curl localhost:3000/metrics/raw | jq '.circuitBreaker'

# Check concurrency adjustments
curl localhost:3000/metrics/raw | jq '.resources.concurrencyChanges'
```

## Best Practices

1. **Monitor the `/metrics` endpoint** regularly for health checks
2. **Set up alerts** on error rate and throughput trends
3. **Track no-match rates** to improve LangGraph query quality
4. **Monitor retry success rates** to optimize retry strategies
5. **Watch circuit breaker patterns** to identify system stress
6. **Use anomaly detection** for proactive issue resolution

## Customization

You can extend the metrics system by:
1. Adding custom metrics to the `MetricsCollector` class
2. Creating custom export formats
3. Adding business-specific quality metrics
4. Implementing custom anomaly detection rules

The metrics system is designed to be lightweight and non-intrusive while providing comprehensive insights into your alert processing pipeline.
