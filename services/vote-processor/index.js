const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

const kafka = new Kafka({
  clientId: 'vote-processor',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
  retry: { retries: 3 }
});

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const globalCounts = {};
const userVotes = new Map();
const votersPerCandidate = {};
const voteChanges = new Map();
const voteHistory = [];
const MAX_HISTORY = 10;
let kafkaConsumer, rabbitConnection, rabbitChannel;
let publishInterval;

async function connectWithRetry(fn, label, delay = 3000) {
  while (true) {
    try { await fn(); console.log(`${label} connected`); return; }
    catch (err) { console.error(`${label} failed: ${err.message}. Retrying in ${delay}ms...`); await new Promise((r) => setTimeout(r, delay)); }
  }
}

function processVote(userId, candidateId) {
  const prevCandidate = userVotes.get(userId);
  if (prevCandidate) {
    globalCounts[prevCandidate]--;
    if (globalCounts[prevCandidate] <= 0) delete globalCounts[prevCandidate];
    if (votersPerCandidate[prevCandidate]) {
      votersPerCandidate[prevCandidate] = votersPerCandidate[prevCandidate].filter(u => u !== userId);
      if (votersPerCandidate[prevCandidate].length === 0) delete votersPerCandidate[prevCandidate];
    }
  }
  globalCounts[candidateId] = (globalCounts[candidateId] || 0) + 1;
  userVotes.set(userId, candidateId);
  if (!votersPerCandidate[candidateId]) votersPerCandidate[candidateId] = [];
  votersPerCandidate[candidateId].push(userId);
  voteChanges.set(userId, !!prevCandidate);
  voteHistory.unshift({ userId, candidate: candidateId, prevCandidate: prevCandidate || null, timestamp: Date.now() });
  if (voteHistory.length > MAX_HISTORY) voteHistory.pop();
}

async function run() {
  kafkaConsumer = kafka.consumer({ groupId: 'global-vote-processor' });
  await connectWithRetry(() => kafkaConsumer.connect(), 'Kafka consumer');
  await kafkaConsumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      let vote;
      try { vote = JSON.parse(message.value.toString()); }
      catch (err) { console.error('Failed to parse vote message:', err.message); return; }
      const userId = message.key.toString();
      const candidate = vote.candidate_id;
      if (!userId || !candidate) { console.error('Vote message missing required fields'); return; }
      processVote(userId, candidate);
      console.log(`Updated global counts:`, globalCounts);
    },
  });

  await connectRabbit();

  publishInterval = setInterval(() => {
    if (Object.keys(globalCounts).length > 0 && rabbitChannel) {
      const changes = {};
      for (const [uid, changed] of voteChanges) {
        const candidate = userVotes.get(uid);
        if (candidate && globalCounts[candidate] !== undefined) changes[uid] = changed;
      }
      const payload = { counts: globalCounts, voters: votersPerCandidate, changes, history: voteHistory };
      rabbitChannel.publish('live_results_global', '', Buffer.from(JSON.stringify(payload)));
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
          try { vote = JSON.parse(message.value.toString()); } catch { return; }
          const userId = message.key.toString();
          const candidate = vote.candidate_id;
          if (userId && candidate) processVote(userId, candidate);
        },
      });
      console.log('Kafka consumer reconnected');
    } catch (e) { console.error('Kafka reconnect failed:', e.message); }
  });
}

async function connectRabbit() {
  rabbitConnection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await rabbitConnection.createChannel();
  await rabbitChannel.assertExchange('live_results_global', 'fanout', { durable: false });

  rabbitConnection.on('error', (err) => { console.error('RabbitMQ error:', err.message); reconnectRabbit(); });
  rabbitConnection.on('close', () => { console.warn('RabbitMQ closed, reconnecting...'); reconnectRabbit(); });
}

async function reconnectRabbit() { await connectWithRetry(connectRabbit, 'RabbitMQ reconnect', 5000); }

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, shutting down...`);
  if (publishInterval) clearInterval(publishInterval);
  const tasks = [];
  if (kafkaConsumer) tasks.push(kafkaConsumer.disconnect().catch(() => {}));
  if (rabbitChannel) tasks.push(rabbitChannel.close().catch(() => {}));
  if (rabbitConnection) tasks.push(rabbitConnection.close().catch(() => {}));
  await Promise.all(tasks);
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

run().catch(console.error);

module.exports = { processVote, globalCounts, userVotes, votersPerCandidate, voteChanges, voteHistory };
