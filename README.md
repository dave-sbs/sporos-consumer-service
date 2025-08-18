# Queue Consumer Service

A robust, production-ready queue consumer service with adaptive concurrency, circuit breaker pattern, and serverless operation capabilities.

## 🚀 Features

- **Adaptive Concurrency**: Automatically adjusts processing capacity based on success rates
- **Circuit Breaker**: Prevents cascade failures with configurable thresholds
- **Dead Letter Queue (DLQ)**: Handles failed messages after max retries
- **Retry Logic**: Exponential backoff with jitter to prevent thundering herd
- **Serverless Mode**: Auto-shutdown when idle to reduce hosting costs
- **HTTP Wake-up**: Can be triggered by external services (e.g., Supabase Edge Functions)
- **State Persistence**: Maintains consumer state across restarts
- **Comprehensive Logging**: Detailed logging for monitoring and debugging

## 🏗️ Architecture

### Serverless Mode (Recommended)
```
Supabase Edge Function → HTTP Call → Consumer Wakes Up → Process Queue → Auto-shutdown
```

### Continuous Mode
```
Consumer runs indefinitely, continuously polling the queue
```

## 📦 Installation

```bash
npm install
```

## ⚙️ Configuration

Copy `env.example` to `.env` and configure:

```bash
# Supabase Configuration
SUPABASE_URL=your_supabase_url_here
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key_here

# Consumer Configuration
CONSUMER_MODE=serverless          # 'serverless' or 'continuous'
IDLE_TIMEOUT=30000               # Milliseconds before auto-shutdown
ENABLE_HTTP=true                 # Enable HTTP wake-up endpoint
PORT=3000                        # HTTP server port
```

## 🚀 Usage

### Development
```bash
npm run dev
```

### Production
```bash
npm run build
npm run start:prod
```

## 🔌 HTTP Endpoints

When `ENABLE_HTTP=true`:

- **POST /wake** - Wake up the consumer (called by edge function)
- **GET /health** - Health check and status information

## 🔄 Serverless Operation

### How It Works
1. **Startup**: Consumer starts and begins processing
2. **Processing**: Continuously processes messages from the queue
3. **Idle Detection**: Monitors for periods of no work
4. **Auto-shutdown**: Shuts down after `IDLE_TIMEOUT` milliseconds of inactivity
5. **Wake-up**: Can be triggered again via HTTP call

### Benefits
- **Cost Effective**: Only pay for actual processing time
- **Resource Efficient**: No idle resource consumption
- **Scalable**: Can be scaled horizontally by running multiple instances

## 🎯 Supabase Edge Function Integration

Your edge function can wake up the consumer when new messages arrive:

```typescript
// In your Supabase Edge Function
const response = await fetch('http://your-consumer:3000/wake', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' }
})

if (response.ok) {
  console.log('Consumer awakened successfully')
}
```

## 📊 Monitoring

### Health Check
```bash
curl http://localhost:3000/health
```

Response:
```json
{
  "status": "healthy",
  "consumerId": "consumer-1234567890-abc123",
  "mode": "serverless",
  "idleTime": 15000
}
```

### Logs
The service provides comprehensive logging:
- Consumer startup/shutdown
- Message processing status
- Circuit breaker state changes
- Concurrency adjustments
- Error details

## 🔧 Customization

### Concurrency Settings
```typescript
private minConcurrency = 1;        // Minimum concurrent processes
private maxConcurrency = 3;        // Maximum concurrent processes
private successThreshold = 0.95;   // Success rate to increase concurrency
```

### Circuit Breaker Settings
```typescript
private circuitBreaker = {
    threshold: 5,                   // Failures before opening circuit
    timeout: 60000,                 // Time before half-open (ms)
    halfOpenMaxCalls: 3            // Test calls in half-open state
}
```

### Retry Settings
```typescript
private maxRetries = 3;            // Maximum retry attempts
// Exponential backoff: 1s → 2s → 4s (max 30s)
// Jitter: Up to 50% randomization
```

## 🚨 Error Handling

- **Message Failures**: Retry with exponential backoff
- **Circuit Breaker**: Prevents cascade failures
- **DLQ**: Failed messages moved to dead letter queue
- **Graceful Shutdown**: Proper cleanup on termination

## 📈 Performance

- **Adaptive Scaling**: Automatically adjusts to system capacity
- **Batch Processing**: Processes multiple messages concurrently
- **Efficient Polling**: Smart delays between queue checks
- **Lock Management**: Prevents duplicate message processing

## 🔒 Security

- **Environment Variables**: Sensitive data stored in `.env`
- **Service Role Key**: Uses Supabase service role for database access
- **HTTP Endpoints**: Simple wake-up mechanism (add auth if needed)

## 🚀 Deployment

### Railway
```bash
# Set environment variables in Railway dashboard
# Deploy with serverless mode enabled
```

### Docker
```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY dist ./dist
EXPOSE 3000
CMD ["npm", "run", "start:prod"]
```

### Environment Variables
Ensure these are set in your deployment environment:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `CONSUMER_MODE=serverless`
- `IDLE_TIMEOUT=30000`

## 🧪 Testing

The service includes simulated processing for testing:
- 50% failure rate (configurable)
- Random processing delays
- Circuit breaker testing scenarios

## 📝 License

MIT License
