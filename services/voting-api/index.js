const express = require('express');
const bodyParser = require('body-parser');
const { Kafka } = require('kafkajs');
const amqp = require('amqplib');
const crypto = require('crypto');

const app = express();
app.use(bodyParser.json());

const kafka = new Kafka({
  clientId: 'voting-api',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
  retry: { retries: 3 }
});
let producer;
const admin = kafka.admin();

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
let amqpConnection, amqpChannel, replyQueue;
let isReady = false;
const pendingRPCs = new Map();
const MAX_PENDING_RPCS = 10000;

async function connectWithRetry(fn, label, delay = 3000) {
  while (true) {
    try {
      await fn();
      console.log(`${label} connected`);
      return;
    } catch (err) {
      console.error(`${label} failed: ${err.message}. Retrying in ${delay}ms...`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
}

async function initKafka() {
  await connectWithRetry(async () => admin.connect(), 'Kafka admin');

  let existingTopics;
  try {
    existingTopics = await admin.listTopics();
  } catch {
    existingTopics = [];
  }

  if (!existingTopics.includes('raw_votes')) {
    try {
      await admin.createTopics({
        topics: [{
          topic: 'raw_votes',
          numPartitions: 1,
          replicationFactor: 1,
          configEntries: [{ name: 'cleanup.policy', value: 'compact' }]
        }]
      });
      console.log('Topic raw_votes created (compacted)');
    } catch (err) {
      console.log('Topic raw_votes may already exist:', err.message);
    }
  }

  try {
    const config = await admin.describeConfigs({
      resources: [{ type: 2, name: 'raw_votes' }],
    });
    const cleanupPolicy = config.resources[0].configEntries.find((c) => c.configName === 'cleanup.policy')?.configValue;
    console.log(`raw_votes cleanup policy: ${cleanupPolicy}`);
    if (cleanupPolicy && cleanupPolicy !== 'compact') {
      await admin.alterConfigs({
        validateOnly: false,
        resources: [
          { type: 2, name: 'raw_votes', configEntries: [{ name: 'cleanup.policy', value: 'compact' }] },
        ],
      });
      console.log('Topic raw_votes updated to compact cleanup policy');
    }
  } catch (err) {
    console.log('Could not check/alter raw_votes config:', err.message);
  }

  await admin.disconnect().catch(() => {});
  await connectWithRetry(async () => {
    producer = kafka.producer();
    await producer.connect();
  }, 'Kafka producer');
}

async function initRabbit() {
  await connectRabbit();
}

async function connectRabbit() {
  amqpConnection = await amqp.connect(RABBITMQ_URL);
  amqpChannel = await amqpConnection.createChannel();
  replyQueue = await amqpChannel.assertQueue('', { exclusive: true });

  amqpChannel.consume(replyQueue.queue, (msg) => {
    if (!msg) return;
    const corrId = msg.properties.correlationId;
    const resolver = pendingRPCs.get(corrId);
    if (resolver) {
      resolver(msg.content.toString());
      pendingRPCs.delete(corrId);
    }
  }, { noAck: true });

  amqpConnection.on('error', (err) => {
    console.error('RabbitMQ connection error:', err.message);
    reconnectRabbit();
  });

  amqpConnection.on('close', () => {
    console.warn('RabbitMQ connection closed, reconnecting...');
    reconnectRabbit();
  });
}

async function reconnectRabbit() {
  pendingRPCs.forEach((_, corrId) => {
    const resolver = pendingRPCs.get(corrId);
    if (resolver) resolver('invalido');
  });
  pendingRPCs.clear();

  await connectWithRetry(connectRabbit, 'RabbitMQ reconnect', 5000);
}

async function validateUserRPC(userId) {
  if (!amqpChannel) throw new Error('RabbitMQ not connected');
  if (pendingRPCs.size >= MAX_PENDING_RPCS) throw new Error('Too many pending RPC requests');

  const correlationId = crypto.randomUUID();

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      pendingRPCs.delete(correlationId);
      reject(new Error('RPC timeout'));
    }, 5000);

    pendingRPCs.set(correlationId, (result) => {
      clearTimeout(timeout);
      resolve(result);
    });

    amqpChannel.sendToQueue('user_validation_queue', Buffer.from(JSON.stringify({ user_id: userId })), {
      correlationId,
      replyTo: replyQueue.queue
    });
  });
}

app.get('/health', (req, res) => {
  res.status(isReady ? 200 : 503).json({ status: isReady ? 'ready' : 'starting' });
});

app.post('/vote', async (req, res) => {
  const { user_id, candidate_id, region, ip_address } = req.body;

  if (typeof user_id !== 'string' || user_id.trim() === '') {
    return res.status(400).json({ error: 'user_id must be a non-empty string' });
  }
  if (typeof candidate_id !== 'string' || candidate_id.trim() === '') {
    return res.status(400).json({ error: 'candidate_id must be a non-empty string' });
  }
  if (typeof region !== 'string' || region.trim() === '') {
    return res.status(400).json({ error: 'region must be a non-empty string' });
  }
  if (typeof ip_address !== 'string' || ip_address.trim() === '') {
    return res.status(400).json({ error: 'ip_address must be a non-empty string' });
  }

  try {
    console.log(`Validating user: ${user_id}`);
    const validationStatus = await validateUserRPC(user_id);

    if (validationStatus === 'valido') {
      console.log(`User ${user_id} is valid. Publishing vote to Kafka...`);
      await producer.send({
        topic: 'raw_votes',
        messages: [
          { key: user_id, value: JSON.stringify({ user_id, candidate_id, region, ip_address, timestamp: Date.now() }) }
        ],
      });
      return res.status(200).json({ message: 'Vote accepted' });
    } else {
      console.log(`User ${user_id} is invalid.`);
      return res.status(403).json({ error: 'User not eligible to vote' });
    }
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

const PORT = process.env.PORT || 3000;
let server;

async function start() {
  await initKafka();
  await initRabbit();
  isReady = true;
  server = app.listen(PORT, () => console.log(`Voting API listening on port ${PORT}`));
}

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, shutting down gracefully...`);
  isReady = false;
  if (server) server.close();

  const shutdownTasks = [];
  if (producer) shutdownTasks.push(producer.disconnect().catch(() => {}));
  if (amqpConnection) shutdownTasks.push(amqpConnection.close().catch(() => {}));

  await Promise.all(shutdownTasks);
  console.log('Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

start().catch((err) => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
