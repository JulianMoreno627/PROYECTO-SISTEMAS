const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

const kafka = new Kafka({
  clientId: 'vote-processor',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
  retry: { retries: 5 }
});

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const globalCounts = {};
const userVotes = new Map();
let kafkaConsumer, rabbitConnection, rabbitChannel;
let publishInterval;

function processVote(userId, candidateId) {
  const prevCandidate = userVotes.get(userId);
  if (prevCandidate) {
    globalCounts[prevCandidate]--;
    if (globalCounts[prevCandidate] <= 0) {
      delete globalCounts[prevCandidate];
    }
  }
  globalCounts[candidateId] = (globalCounts[candidateId] || 0) + 1;
  userVotes.set(userId, candidateId);
}

async function run() {
  // Kafka Consumer
  kafkaConsumer = kafka.consumer({ groupId: 'global-vote-processor' });
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      let vote;
      try {
        vote = JSON.parse(message.value.toString());
      } catch (err) {
        console.error('Failed to parse vote message:', err.message);
        return;
      }
      const userId = message.key.toString();
      const candidate = vote.candidate_id;

      if (!userId || !candidate) {
        console.error('Vote message missing required fields');
        return;
      }

      processVote(userId, candidate);
      console.log(`Updated global counts:`, globalCounts);
    },
  });

  // RabbitMQ Publisher
  await connectRabbit();

  publishInterval = setInterval(() => {
    if (Object.keys(globalCounts).length > 0 && rabbitChannel) {
      rabbitChannel.publish('live_results_global', '', Buffer.from(JSON.stringify(globalCounts)));
      console.log('Published global counts to RabbitMQ Fanout');
    }
  }, 1000);

  kafkaConsumer.on('consumer.crash', async (err) => {
    console.error('Kafka consumer crashed:', err.message);
    try {
      kafkaConsumer = kafka.consumer({ groupId: 'global-vote-processor' });
      await kafkaConsumer.connect();
      await kafkaConsumer.subscribe({ topic: 'raw_votes', fromBeginning: true });
      await kafkaConsumer.run({
        eachMessage: async ({ message }) => {
          let vote;
          try {
            vote = JSON.parse(message.value.toString());
          } catch (parseErr) {
            console.error('Failed to parse vote message:', parseErr.message);
            return;
          }
          const userId = message.key.toString();
          const candidate = vote.candidate_id;
          if (userId && candidate) {
            processVote(userId, candidate);
          }
        },
      });
      console.log('Kafka consumer reconnected');
    } catch (reconnectErr) {
      console.error('Kafka reconnect failed:', reconnectErr.message);
    }
  });
}

async function connectRabbit() {
  rabbitConnection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await rabbitConnection.createChannel();
  const exchange = 'live_results_global';

  await rabbitChannel.assertExchange(exchange, 'fanout', { durable: false });

  rabbitConnection.on('error', (err) => {
    console.error('RabbitMQ connection error:', err.message);
    reconnectRabbit();
  });

  rabbitConnection.on('close', () => {
    console.warn('RabbitMQ connection closed, reconnecting...');
    reconnectRabbit();
  });

  console.log('RabbitMQ connected');
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
  if (publishInterval) clearInterval(publishInterval);

  const shutdownTasks = [];

  if (kafkaConsumer) {
    shutdownTasks.push(kafkaConsumer.disconnect().catch(err => console.error('Kafka disconnect error:', err.message)));
  }

  if (rabbitChannel) {
    shutdownTasks.push(rabbitChannel.close().catch(err => console.error('RabbitMQ channel close error:', err.message)));
  }

  if (rabbitConnection) {
    shutdownTasks.push(rabbitConnection.close().catch(err => console.error('RabbitMQ close error:', err.message)));
  }

  await Promise.all(shutdownTasks);
  console.log('Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

run().catch(console.error);

module.exports = { processVote, globalCounts, userVotes };
