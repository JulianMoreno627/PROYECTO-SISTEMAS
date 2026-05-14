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
  const exchange = 'live_results_regional';

  await channel.assertExchange(exchange, 'topic', { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });
  
  // Subscribe to all regions
  await channel.bindQueue(q.queue, exchange, 'results.*');

  channel.consume(q.queue, (msg) => {
    const data = JSON.parse(msg.content.toString());
    const region = msg.fields.routingKey.split('.')[1];
    const payload = JSON.stringify({ region, results: data });
    
    console.log(`Received regional update for ${region}, broadcasting`);
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
      }
    });
  }, { noAck: true });

  const PORT = process.env.PORT || 4001;
  server.listen(PORT, () => console.log(`Regional Dashboard running on http://localhost:${PORT}`));
}

run().catch(console.error);
