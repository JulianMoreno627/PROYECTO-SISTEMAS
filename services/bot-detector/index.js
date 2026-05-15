const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'bot-detector-service',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});

const producer = kafka.producer();
const ipVotersMap = new Map(); // ip -> { set: Set<userId>, firstVoteTime: Number, alerted: boolean }
const CLEANUP_INTERVAL_MS = 120000; // 2 minutes
const STALE_THRESHOLD_MS = 300000; // 5 minutes

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
    if (now - entry.firstVoteTime > STALE_THRESHOLD_MS) {
      ipVotersMap.delete(ip);
    }
  }
}

async function run() {
  await producer.connect();
  const consumer = kafka.consumer({ groupId: 'bot-detector-group' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const vote = JSON.parse(message.value.toString());
      const now = Date.now();

      const alert = processVote(vote.ip_address, vote.user_id, now);
      if (alert) {
        console.warn('ALERT:', alert.message);
        await producer.send({
          topic: 'security_alerts',
          messages: [{ value: JSON.stringify(alert) }]
        });
      }
    },
  });

  setInterval(cleanupStaleIPs, CLEANUP_INTERVAL_MS);
}

run().catch(console.error);

module.exports = { processVote, cleanupStaleIPs, ipVotersMap, CLEANUP_INTERVAL_MS, STALE_THRESHOLD_MS };
