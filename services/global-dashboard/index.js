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

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

async function run() {
  await connectRabbit();
}

async function connectRabbit() {
  rabbitConnection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await rabbitConnection.createChannel();
  const exchange = 'live_results_global';

  await rabbitChannel.assertExchange(exchange, 'fanout', { durable: false });
  const q = await rabbitChannel.assertQueue('', { exclusive: true });
  await rabbitChannel.bindQueue(q.queue, exchange, '');

  rabbitChannel.consume(q.queue, (msg) => {
    if (!msg) return;
    const data = msg.content.toString();
    try {
      JSON.parse(data);
    } catch (err) {
      console.error('Failed to parse global update:', err.message);
      return;
    }
    console.log('Received global update, broadcasting to clients');
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(data);
      }
    });
  }, { noAck: true });

  rabbitConnection.on('error', (err) => {
    console.error('RabbitMQ connection error:', err.message);
    reconnectRabbit();
  });

  rabbitConnection.on('close', () => {
    console.warn('RabbitMQ connection closed, reconnecting...');
    reconnectRabbit();
  });

  const PORT = process.env.PORT || 4000;
  server.listen(PORT, () => console.log(`Global Dashboard running on http://localhost:${PORT}`));
}

async function reconnectRabbit() {
  try {
    await connectRabbit();
  } catch (err) {
    console.error('RabbitMQ reconnect failed, retrying in 5s:', err.message);
    setTimeout(reconnectRabbit, 5000);
  }
}

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, shutting down gracefully...`);

  const shutdownTasks = [];

  if (rabbitChannel) {
    shutdownTasks.push(rabbitChannel.close().catch(err => console.error('RabbitMQ channel close error:', err.message)));
  }

  if (rabbitConnection) {
    shutdownTasks.push(rabbitConnection.close().catch(err => console.error('RabbitMQ close error:', err.message)));
  }

  wss.clients.forEach(client => client.close());

  await Promise.all(shutdownTasks);
  server.close();
  console.log('Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
