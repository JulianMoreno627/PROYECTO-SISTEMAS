const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

const kafka = new Kafka({
  clientId: 'regional-rollup-service',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
  retry: { retries: 3 }
});

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const regionalCounts = {};
const userVotes = new Map();
const regionVoters = {};
const voteChanges = new Map();
let kafkaConsumer, rabbitConnection, rabbitChannel;
let publishInterval;

async function connectWithRetry(fn, label, delay = 3000) {
  while (true) {
    try { await fn(); console.log(`${label} connected`); return; }
    catch (err) { console.error(`${label} failed: ${err.message}. Retrying in ${delay}ms...`); await new Promise((r) => setTimeout(r, delay)); }
  }
}

function processVote(userId, region, candidateId) {
  const prevVote = userVotes.get(userId);
  if (prevVote && regionalCounts[prevVote.region]) {
    regionalCounts[prevVote.region][prevVote.candidate_id]--;
    if (regionalCounts[prevVote.region][prevVote.candidate_id] <= 0) delete regionalCounts[prevVote.region][prevVote.candidate_id];
    if (Object.keys(regionalCounts[prevVote.region]).length === 0) delete regionalCounts[prevVote.region];
    if (regionVoters[prevVote.region] && regionVoters[prevVote.region][prevVote.candidate_id]) {
      regionVoters[prevVote.region][prevVote.candidate_id] = regionVoters[prevVote.region][prevVote.candidate_id].filter(u => u !== userId);
      if (regionVoters[prevVote.region][prevVote.candidate_id].length === 0) delete regionVoters[prevVote.region][prevVote.candidate_id];
      if (Object.keys(regionVoters[prevVote.region]).length === 0) delete regionVoters[prevVote.region];
    }
  }
  if (!regionalCounts[region]) regionalCounts[region] = {};
  regionalCounts[region][candidateId] = (regionalCounts[region][candidateId] || 0) + 1;
  userVotes.set(userId, { region, candidate_id: candidateId });
  if (!regionVoters[region]) regionVoters[region] = {};
  if (!regionVoters[region][candidateId]) regionVoters[region][candidateId] = [];
  regionVoters[region][candidateId].push(userId);
  voteChanges.set(userId, !!prevVote);
}

function getRoutingKey(region) {
  return `results.${region.toLowerCase().replace(/\s+/g, '_')}`;
}

async function run() {
  kafkaConsumer = kafka.consumer({ groupId: 'regional-rollup-processor' });
  await connectWithRetry(() => kafkaConsumer.connect(), 'Kafka consumer');
  await kafkaConsumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      let vote;
      try { vote = JSON.parse(message.value.toString()); }
      catch (err) { console.error('Failed to parse vote message:', err.message); return; }
      const userId = message.key.toString();
      const region = vote.region;
      const candidate = vote.candidate_id;
      if (!userId || !region || !candidate) { console.error('Vote message missing required fields'); return; }
      processVote(userId, region, candidate);
      console.log(`Updated regional counts for ${region}:`, regionalCounts[region]);
    },
  });

  await connectRabbit();

  publishInterval = setInterval(() => {
    if (Object.keys(regionalCounts).length > 0 && rabbitChannel) {
      for (const region in regionalCounts) {
        const routingKey = getRoutingKey(region);
        const changes = {};
        for (const [uid, changed] of voteChanges) {
          const vote = userVotes.get(uid);
          if (vote && vote.region === region && regionalCounts[region][vote.candidate_id] !== undefined) changes[uid] = changed;
        }
        const payload = { counts: regionalCounts[region], voters: regionVoters[region] || {}, changes };
        rabbitChannel.publish('live_results_regional', routingKey, Buffer.from(JSON.stringify(payload)));
        console.log(`Published regional counts for ${region} to RabbitMQ Topic with key ${routingKey}`);
      }
    }
  }, 5000);

  kafkaConsumer.on('consumer.crash', async (err) => {
    console.error('Kafka consumer crashed:', err.message);
    try {
      kafkaConsumer = kafka.consumer({ groupId: 'regional-rollup-processor' });
      await kafkaConsumer.connect();
      await kafkaConsumer.subscribe({ topic: 'raw_votes', fromBeginning: true });
      await kafkaConsumer.run({
        eachMessage: async ({ message }) => {
          let vote;
          try { vote = JSON.parse(message.value.toString()); } catch { return; }
          const userId = message.key.toString();
          const region = vote.region;
          const candidate = vote.candidate_id;
          if (userId && region && candidate) processVote(userId, region, candidate);
        },
      });
      console.log('Kafka consumer reconnected');
    } catch (e) { console.error('Kafka reconnect failed:', e.message); }
  });
}

async function connectRabbit() {
  rabbitConnection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await rabbitConnection.createChannel();
  await rabbitChannel.assertExchange('live_results_regional', 'topic', { durable: false });

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

module.exports = { processVote, getRoutingKey, regionalCounts, userVotes, regionVoters, voteChanges };
