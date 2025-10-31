// consumer.js - Node.js Kafka Consumer with Elasticsearch
const { Kafka } = require('kafkajs');
const { Client } = require('@elastic/elasticsearch');
const express = require('express');
const promClient = require('prom-client');

// Prometheus metrics setup
const register = new promClient.Registry();
promClient.collectDefaultMetrics({ register });

const ordersConsumed = new promClient.Counter({
  name: 'orders_consumed_total',
  help: 'Total orders consumed',
  registers: [register]
});

const ordersIndexed = new promClient.Counter({
  name: 'orders_indexed_total',
  help: 'Total orders indexed to Elasticsearch',
  registers: [register]
});

const consumptionErrors = new promClient.Counter({
  name: 'consumption_errors_total',
  help: 'Total consumption errors',
  registers: [register]
});

const processingLatency = new promClient.Histogram({
  name: 'processing_latency_seconds',
  help: 'Processing latency in seconds',
  registers: [register]
});

const consumerLag = new promClient.Gauge({
  name: 'consumer_lag',
  help: 'Consumer lag in messages',
  labelNames: ['topic', 'partition'],
  registers: [register]
});

// Kafka setup
const kafka = new Kafka({
  clientId: 'order-consumer',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

const consumer = kafka.consumer({ 
  groupId: 'order-processor-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000
});

// Elasticsearch setup
const esClient = new Client({
  node: process.env.ELASTICSEARCH_HOST || 'http://localhost:9200',
  requestTimeout: 30000,
  maxRetries: 3
});

// Statistics
const stats = {
  totalConsumed: 0,
  totalIndexed: 0,
  totalRevenue: 0,
  byRegion: {},
  byStatus: {},
  byCategory: {},
  startTime: Date.now()
};

// Create Elasticsearch index
async function createIndex() {
  try {
    const indexExists = await esClient.indices.exists({ index: 'orders' });
    
    if (!indexExists) {
      await esClient.indices.create({
        index: 'orders',
        body: {
          mappings: {
            properties: {
              order_id: { type: 'keyword' },
              timestamp: { type: 'date' },
              product_id: { type: 'keyword' },
              product_name: { type: 'text' },
              category: { type: 'keyword' },
              quantity: { type: 'integer' },
              price: { type: 'float' },
              total_amount: { type: 'float' },
              region: { type: 'keyword' },
              status: { type: 'keyword' },
              payment_method: { type: 'keyword' },
              customer_id: { type: 'keyword' },
              processed_at: { type: 'date' },
              'shipping_address.country': { type: 'keyword' },
              'shipping_address.city': { type: 'keyword' },
              'shipping_address.postal_code': { type: 'keyword' }
            }
          },
          settings: {
            number_of_shards: 3,
            number_of_replicas: 1
          }
        }
      });
      console.log('✓ Elasticsearch index "orders" created');
    } else {
      console.log('✓ Elasticsearch index "orders" already exists');
    }
  } catch (error) {
    console.error('✗ Error creating index:', error.message);
    throw error;
  }
}

// Process and index order
async function processOrder(order) {
  const end = processingLatency.startTimer();
  
  try {
    // Add processing metadata
    order.processed_at = new Date().toISOString();
    
    // Index to Elasticsearch
    await esClient.index({
      index: 'orders',
      id: order.order_id,
      body: order,
      refresh: false // Async refresh for better performance
    });
    
    ordersIndexed.inc();
    end();
    
    // Update statistics
    stats.totalIndexed++;
    stats.totalRevenue += order.total_amount;
    stats.byRegion[order.region] = (stats.byRegion[order.region] || 0) + 1;
    stats.byStatus[order.status] = (stats.byStatus[order.status] || 0) + 1;
    stats.byCategory[order.category] = (stats.byCategory[order.category] || 0) + 1;
    
    console.log(`✓ Processed: ${order.order_id} | ${order.product_name} | $${order.total_amount} | ${order.region}`);
    
  } catch (error) {
    consumptionErrors.inc();
    end();
    console.error(`✗ Failed to process order ${order.order_id}:`, error.message);
    throw error;
  }
}

// Start consumer
async function startConsumer() {
  try {
    await consumer.connect();
    console.log('✓ Kafka Consumer connected');
    
    await consumer.subscribe({ 
      topic: 'orders', 
      fromBeginning: false 
    });
    console.log('✓ Subscribed to topic: orders');
    
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const order = JSON.parse(message.value.toString());
          
          ordersConsumed.inc();
          stats.totalConsumed++;
          
          await processOrder(order);
          
        } catch (error) {
          consumptionErrors.inc();
          console.error('Error processing message:', error.message);
        }
      }
    });
    
    console.log('🚀 Consumer running - processing orders...');
    
  } catch (error) {
    console.error('Failed to start consumer:', error);
    process.exit(1);
  }
}

// Monitor consumer lag
async function monitorLag() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    
    setInterval(async () => {
      try {
        const offsets = await admin.fetchOffsets({
          groupId: 'order-processor-group',
          topics: ['orders']
        });
        
        for (const topic of offsets) {
          for (const partition of topic.partitions) {
            const lag = parseInt(partition.high) - parseInt(partition.offset);
            consumerLag.set(
              { topic: topic.topic, partition: partition.partition },
              lag
            );
          }
        }
      } catch (error) {
        console.error('Error fetching lag:', error.message);
      }
    }, 10000); // Check every 10 seconds
    
  } catch (error) {
    console.error('Failed to setup lag monitoring:', error.message);
  }
}

// Print statistics periodically
function startStatsLogger() {
  setInterval(() => {
    const uptime = Math.floor((Date.now() - stats.startTime) / 1000);
    const avgRevenue = stats.totalConsumed > 0 ? (stats.totalRevenue / stats.totalConsumed).toFixed(2) : 0;
    
    console.log('\n' + '='.repeat(80));
    console.log('📊 CONSUMER STATISTICS');
    console.log('='.repeat(80));
    console.log(`Uptime: ${uptime}s | Consumed: ${stats.totalConsumed} | Indexed: ${stats.totalIndexed}`);
    console.log(`Total Revenue: $${stats.totalRevenue.toFixed(2)} | Avg Order: $${avgRevenue}`);
    console.log(`By Region: ${JSON.stringify(stats.byRegion)}`);
    console.log(`By Status: ${JSON.stringify(stats.byStatus)}`);
    console.log(`By Category: ${JSON.stringify(stats.byCategory)}`);
    console.log('='.repeat(80) + '\n');
  }, 30000); // Every 30 seconds
}

// Express server for metrics and API
const app = express();
const port = process.env.CONSUMER_PORT || 3002;

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    service: 'consumer',
    stats: stats
  });
});

app.get('/stats', (req, res) => {
  res.json({
    ...stats,
    uptime: Math.floor((Date.now() - stats.startTime) / 1000),
    avgRevenue: stats.totalConsumed > 0 ? (stats.totalRevenue / stats.totalConsumed).toFixed(2) : 0
  });
});

// Search orders in Elasticsearch
app.get('/search', async (req, res) => {
  try {
    const { query, region, status, from = 0, size = 10 } = req.query;
    
    const searchBody = {
      query: {
        bool: {
          must: [],
          filter: []
        }
      },
      sort: [{ timestamp: 'desc' }],
      from: parseInt(from),
      size: parseInt(size)
    };
    
    if (query) {
      searchBody.query.bool.must.push({
        multi_match: {
          query: query,
          fields: ['product_name', 'order_id', 'customer_id']
        }
      });
    }
    
    if (region) {
      searchBody.query.bool.filter.push({ term: { region } });
    }
    
    if (status) {
      searchBody.query.bool.filter.push({ term: { status } });
    }
    
    const result = await esClient.search({
      index: 'orders',
      body: searchBody
    });
    
    res.json({
      total: result.hits.total.value,
      orders: result.hits.hits.map(hit => hit._source)
    });
    
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Graceful shutdown
async function shutdown() {
  console.log('\n⏹ Shutting down consumer...');
  await consumer.disconnect();
  await esClient.close();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// Start everything
app.listen(port, async () => {
  console.log(`📊 Metrics server running on port ${port}`);
  console.log(`   Metrics: http://localhost:${port}/metrics`);
  console.log(`   Health: http://localhost:${port}/health`);
  console.log(`   Stats: http://localhost:${port}/stats`);
  console.log(`   Search: http://localhost:${port}/search?query=laptop`);
  
  await createIndex();
  await startConsumer();
  monitorLag();
  startStatsLogger();
});

module.exports = { consumer, processOrder };