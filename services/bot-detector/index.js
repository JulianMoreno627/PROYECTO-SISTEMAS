const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'bot-detector-service',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
  retry: { retries: 3 }
});

let producer, kafkaConsumer;
const ipVotersMap = new Map();
const CLEANUP_INTERVAL_MS = 120000;
const STALE_THRESHOLD_MS = 300000;
let cleanupInterval;

async function connectWithRetry(fn, label, delay = 3000) {
  while (true) {
    try { await fn(); console.log(`${label} connected`); return; }
    catch (err) { console.error(`${label} failed: ${err.message}. Retrying in ${delay}ms...`); await new Promise((r) => setTimeout(r, delay)); }
  }
}

function processVote(ipAddress, userId, timestamp) {
  const now = timestamp || Date.now();
  if (!ipVotersMap.has(ipAddress)) {
    ipVotersMap.set(ipAddress, { set: new Set(), firstVoteTime: now, alerted: false });
  }
  const entry = ipVotersMap.get(ipAddress);
  if (now - entry.firstVoteTime > 60000) {
    entry.set.clear();
    entry.firstVoteTime = now;
    entry.alerted = false;
  }
  entry.set.add(userId);
  if (entry.set.size > 5 && !entry.alerted) {
    entry.alerted = true;
    return {
      ip: ipAddress,
      users: Array.from(entry.set),
      message: 'Potential bot attack detected: >5 users from same IP in 1 min',
      timestamp: now,
    };
  }
  return null;
}

function cleanupStaleIPs(timestamp) {
  const now = timestamp || Date.now();
  for (const [ip, entry] of ipVotersMap) {
    if (now - entry.firstVoteTime > STALE_THRESHOLD_MS) ipVotersMap.delete(ip);
  }
}

async function run() {
  const admin = kafka.admin();
  await connectWithRetry(() => admin.connect(), 'Kafka admin');

  try {
    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes('security_alerts')) {
      await admin.createTopics({
        topics: [{ topic: 'security_alerts', numPartitions: 1, replicationFactor: 1 }]
      });
      console.log('Topic security_alerts created');
    }
  } catch (err) {
    console.log('Topic security_alerts may already exist:', err.message);
  }
  await admin.disconnect().catch(() => {});

  producer = kafka.producer();
  await connectWithRetry(() => producer.connect(), 'Kafka producer');

  kafkaConsumer = kafka.consumer({ groupId: 'bot-detector-group' });
  await connectWithRetry(() => kafkaConsumer.connect(), 'Kafka consumer');
  await kafkaConsumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      let vote;
      try { vote = JSON.parse(message.value.toString()); }
      catch (err) { console.error('Failed to parse vote message:', err.message); return; }
      if (!vote.ip_address || !vote.user_id) { console.error('Vote message missing required fields'); return; }
      const alert = processVote(vote.ip_address, vote.user_id, Date.now());
      if (alert) {
        console.warn('ALERT:', alert.message);
        await producer.send({ topic: 'security_alerts', messages: [{ value: JSON.stringify(alert) }] });
      }
    },
  });

  cleanupInterval = setInterval(cleanupStaleIPs, CLEANUP_INTERVAL_MS);

  kafkaConsumer.on('consumer.crash', async (err) => {
    console.error('Kafka consumer crashed:', err.message);
    try {
      kafkaConsumer = kafka.consumer({ groupId: 'bot-detector-group' });
      await kafkaConsumer.connect();
      await kafkaConsumer.subscribe({ topic: 'raw_votes', fromBeginning: true });
      await kafkaConsumer.run({
        eachMessage: async ({ message }) => {
          let vote;
          try { vote = JSON.parse(message.value.toString()); } catch { return; }
          if (vote.ip_address && vote.user_id) {
            const alert = processVote(vote.ip_address, vote.user_id, Date.now());
            if (alert) {
              console.warn('ALERT:', alert.message);
              await producer.send({ topic: 'security_alerts', messages: [{ value: JSON.stringify(alert) }] });
            }
          }
        },
      });
      console.log('Kafka consumer reconnected');
    } catch (e) { console.error('Kafka reconnect failed:', e.message); }
  });

  producer.on('producer.disconnect', async () => {
    console.error('Kafka producer disconnected, reconnecting...');
    try {
      producer = kafka.producer();
      await producer.connect();
      console.log('Kafka producer reconnected');
    } catch (e) { console.error('Producer reconnect failed:', e.message); }
  });
}

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, shutting down...`);
  if (cleanupInterval) clearInterval(cleanupInterval);
  const tasks = [];
  if (kafkaConsumer) tasks.push(kafkaConsumer.disconnect().catch(() => {}));
  if (producer) tasks.push(producer.disconnect().catch(() => {}));
  await Promise.all(tasks);
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

run().catch(console.error);

module.exports = { processVote, cleanupStaleIPs, ipVotersMap, CLEANUP_INTERVAL_MS, STALE_THRESHOLD_MS };
