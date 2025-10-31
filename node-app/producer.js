// producer.js - Node.js Kafka Producer with Prometheus Metrics
const { Kafka } = require('kafkajs');
const express = require('express');
const promClient = require('prom-client');

// Prometheus metrics setup
const register = new promClient.Registry();
promClient.collectDefaultMetrics({ register });

const ordersProduced = new promClient.Counter({
  name: 'orders_produced_total',
  help: 'Total number of orders produced',
  registers: [register]
});

const productionErrors = new promClient.Counter({
  name: 'production_errors_total',
  help: 'Total production errors',
  registers: [register]
});

const productionLatency = new promClient.Histogram({
  name: 'production_latency_seconds',
  help: 'Production latency in seconds',
  registers: [register]
});

// Kafka setup
const kafka = new Kafka({
  clientId: 'order-producer',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

const producer = kafka.producer({
  allowAutoTopicCreation: true,
  transactionTimeout: 30000
});

// Sample data
const products = [
  { id: 'P001', name: 'Laptop', price: 999.99, category: 'Electronics' },
  { id: 'P002', name: 'Smartphone', price: 699.99, category: 'Electronics' },
  { id: 'P003', name: 'Headphones', price: 149.99, category: 'Audio' },
  { id: 'P004', name: 'Smart Watch', price: 299.99, category: 'Wearables' },
  { id: 'P005', name: 'Tablet', price: 499.99, category: 'Electronics' },
  { id: 'P006', name: 'Camera', price: 799.99, category: 'Photography' },
  { id: 'P007', name: 'Speaker', price: 199.99, category: 'Audio' },
  { id: 'P008', name: 'Monitor', price: 349.99, category: 'Electronics' },
  { id: 'P009', name: 'Keyboard', price: 89.99, category: 'Accessories' },
  { id: 'P010', name: 'Mouse', price: 49.99, category: 'Accessories' }
];

const regions = ['US-East', 'US-West', 'EU-Central', 'Asia-Pacific', 'South-America'];
const statuses = ['pending', 'confirmed', 'processing', 'shipped', 'delivered'];
const paymentMethods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer'];

// Generate random order
function generateOrder() {
  const product = products[Math.floor(Math.random() * products.length)];
  const quantity = Math.floor(Math.random() * 5) + 1;
  const totalAmount = parseFloat((product.price * quantity).toFixed(2));
  
  return {
    order_id: `ORD${Date.now()}${Math.floor(Math.random() * 1000)}`,
    timestamp: new Date().toISOString(),
    product_id: product.id,
    product_name: product.name,
    category: product.category,
    quantity: quantity,
    price: product.price,
    total_amount: totalAmount,
    region: regions[Math.floor(Math.random() * regions.length)],
    status: statuses[Math.floor(Math.random() * statuses.length)],
    payment_method: paymentMethods[Math.floor(Math.random() * paymentMethods.length)],
    customer_id: `CUST${Math.floor(Math.random() * 10000)}`,
    shipping_address: {
      country: regions[Math.floor(Math.random() * regions.length)].split('-')[0],
      city: `City${Math.floor(Math.random() * 100)}`,
      postal_code: `${Math.floor(Math.random() * 90000) + 10000}`
    }
  };
}

// Send order to Kafka
async function sendOrder(order) {
  const end = productionLatency.startTimer();
  
  try {
    const result = await producer.send({
      topic: 'orders',
      messages: [
        {
          key: order.order_id,
          value: JSON.stringify(order),
          headers: {
            'correlation-id': order.order_id,
            'source': 'order-producer'
          }
        }
      ]
    });
    
    ordersProduced.inc();
    end();
    
    console.log(`✓ Order sent: ${order.order_id} | Product: ${order.product_name} | Amount: $${order.total_amount} | Partition: ${result[0].partition}`);
    
    return result;
  } catch (error) {
    productionErrors.inc();
    end();
    console.error(`✗ Failed to send order ${order.order_id}:`, error.message);
    throw error;
  }
}

// Start producer
async function startProducer() {
  try {
    await producer.connect();
    console.log('✓ Kafka Producer connected');
    
    // Send orders at specified interval
    const interval = parseInt(process.env.PRODUCER_INTERVAL || '2000');
    
    setInterval(async () => {
      try {
        const order = generateOrder();
        await sendOrder(order);
      } catch (error) {
        console.error('Error in producer loop:', error.message);
      }
    }, interval);
    
    console.log(`🚀 Producer running - sending orders every ${interval}ms`);
    
  } catch (error) {
    console.error('Failed to start producer:', error);
    process.exit(1);
  }
}

// Express server for metrics
const app = express();
const port = process.env.PRODUCER_PORT || 3001;

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.get('/health', (req, res) => {
  res.json({ status: 'healthy', service: 'producer' });
});

app.get('/produce', async (req, res) => {
  try {
    const count = parseInt(req.query.count || '1');
    const orders = [];
    
    for (let i = 0; i < count; i++) {
      const order = generateOrder();
      await sendOrder(order);
      orders.push(order.order_id);
    }
    
    res.json({ 
      success: true, 
      count: count,
      orders: orders
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// Graceful shutdown
async function shutdown() {
  console.log('\n⏹ Shutting down producer...');
  await producer.disconnect();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// Start everything
app.listen(port, () => {
  console.log(`📊 Metrics server running on port ${port}`);
  console.log(`   Metrics: http://localhost:${port}/metrics`);
  console.log(`   Health: http://localhost:${port}/health`);
  console.log(`   Manual produce: http://localhost:${port}/produce?count=10`);
  startProducer();
});

module.exports = { producer, generateOrder, sendOrder };