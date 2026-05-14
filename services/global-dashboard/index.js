const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const amqp = require('amqplib');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

async function run() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();
  const exchange = 'live_results_global';

  await channel.assertExchange(exchange, 'fanout', { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(q.queue, exchange, '');

  channel.consume(q.queue, (msg) => {
    const data = msg.content.toString();
    console.log('Received global update, broadcasting to clients');
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(data);
      }
    });
  }, { noAck: true });

  const PORT = process.env.PORT || 4000;
  server.listen(PORT, () => console.log(`Global Dashboard running on http://localhost:${PORT}`));
}

run().catch(console.error);
