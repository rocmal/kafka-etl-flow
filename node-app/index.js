// index.js - Main entry point that runs both producer and consumer
const { spawn } = require('child_process');
const path = require('path');

console.log('🚀 Starting Kafka Node.js Application...\n');

// Start producer
const producer = spawn('node', [path.join(__dirname, 'producer.js')], {
  stdio: 'inherit',
  env: { ...process.env, PRODUCER_PORT: '3001' }
});

producer.on('error', (error) => {
  console.error('Failed to start producer:', error);
});

producer.on('exit', (code) => {
  console.log(`Producer process exited with code ${code}`);
});

// Wait 5 seconds before starting consumer
setTimeout(() => {
  const consumer = spawn('node', [path.join(__dirname, 'consumer.js')], {
    stdio: 'inherit',
    env: { ...process.env, CONSUMER_PORT: '3002' }
  });

  consumer.on('error', (error) => {
    console.error('Failed to start consumer:', error);
  });

  consumer.on('exit', (code) => {
    console.log(`Consumer process exited with code ${code}`);
  });
}, 5000);

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('Received SIGTERM, shutting down...');
  producer.kill();
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('Received SIGINT, shutting down...');
  producer.kill();
  process.exit(0);
});