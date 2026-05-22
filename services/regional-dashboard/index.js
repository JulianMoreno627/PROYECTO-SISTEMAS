const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const amqp = require('amqplib');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
let rabbitConnection, rabbitChannel;

async function connectWithRetry(fn, label, delay = 3000) {
  while (true) {
    try { await fn(); console.log(`${label} connected`); return; }
    catch (err) { console.error(`${label} failed: ${err.message}. Retrying in ${delay}ms...`); await new Promise((r) => setTimeout(r, delay)); }
  }
}

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

async function run() {
  await connectWithRetry(connectRabbit, 'RabbitMQ');
  const PORT = process.env.PORT || 4001;
  server.listen(PORT, () => console.log(`Regional Dashboard running on http://localhost:${PORT}`));
}

async function connectRabbit() {
  rabbitConnection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await rabbitConnection.createChannel();
  const exchange = 'live_results_regional';

  await rabbitChannel.assertExchange(exchange, 'topic', { durable: false });
  const q = await rabbitChannel.assertQueue('', { exclusive: true });
  await rabbitChannel.bindQueue(q.queue, exchange, 'results.*');

  rabbitChannel.consume(q.queue, (msg) => {
    if (!msg) return;
    let data;
    try { data = JSON.parse(msg.content.toString()); }
    catch (err) { console.error('Failed to parse regional update:', err.message); return; }
    const region = msg.fields.routingKey.split('.')[1];
    const payload = JSON.stringify({ region, results: data });
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) client.send(payload);
    });
  }, { noAck: true });

  rabbitConnection.on('error', (err) => { console.error('RabbitMQ error:', err.message); reconnectRabbit(); });
  rabbitConnection.on('close', () => { console.warn('RabbitMQ closed, reconnecting...'); reconnectRabbit(); });
}

async function reconnectRabbit() { await connectWithRetry(connectRabbit, 'RabbitMQ reconnect', 5000); }

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, shutting down...`);
  const tasks = [];
  if (rabbitChannel) tasks.push(rabbitChannel.close().catch(() => {}));
  if (rabbitConnection) tasks.push(rabbitConnection.close().catch(() => {}));
  wss.clients.forEach(client => client.close());
  await Promise.all(tasks);
  server.close();
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

run().catch(console.error);
