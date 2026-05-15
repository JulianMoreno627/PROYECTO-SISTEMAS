const express = require('express');
const bodyParser = require('body-parser');
const { Kafka } = require('kafkajs');
const amqp = require('amqplib');
const crypto = require('crypto');

const app = express();
app.use(bodyParser.json());

const kafka = new Kafka({
  clientId: 'voting-api',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});
const producer = kafka.producer();
const admin = kafka.admin();

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
let amqpChannel, replyQueue;
const pendingRPCs = new Map();

async function initKafka() {
  await admin.connect();
  const existingTopics = await admin.listTopics();
  if (!existingTopics.includes('raw_votes')) {
    await admin.createTopics({
      topics: [{
        topic: 'raw_votes',
        numPartitions: 1,
        replicationFactor: 1,
        configEntries: [{ name: 'cleanup.policy', value: 'compact' }]
      }]
    });
    console.log('Topic raw_votes created (compacted)');
  } else {
    console.log('Topic raw_votes already exists');
  }
  await admin.disconnect();
  await producer.connect();
}

async function initRabbit() {
  const conn = await amqp.connect(RABBITMQ_URL);
  amqpChannel = await conn.createChannel();
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
}

async function validateUserRPC(userId) {
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

app.post('/vote', async (req, res) => {
  const { user_id, candidate_id, region, ip_address } = req.body;

  if (!user_id || !candidate_id || !region || !ip_address) {
    return res.status(400).json({ error: 'Missing fields' });
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
async function start() {
  await initKafka();
  await initRabbit();
  app.listen(PORT, () => console.log(`Voting API listening on port ${PORT}`));
}

start().catch(console.error);
