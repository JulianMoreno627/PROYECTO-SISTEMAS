const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'bot-detector-service',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
  retry: { retries: 5 }
});

let producer, kafkaConsumer;
const ipVotersMap = new Map();
const CLEANUP_INTERVAL_MS = 120000;
const STALE_THRESHOLD_MS = 300000;
let cleanupInterval;

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
  producer = kafka.producer();
  await producer.connect();

  kafkaConsumer = kafka.consumer({ groupId: 'bot-detector-group' });
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
      const now = Date.now();

      if (!vote.ip_address || !vote.user_id) {
        console.error('Vote message missing required fields');
        return;
      }

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
          try {
            vote = JSON.parse(message.value.toString());
          } catch (parseErr) {
            console.error('Failed to parse vote message:', parseErr.message);
            return;
          }
          const now = Date.now();
          if (vote.ip_address && vote.user_id) {
            const alert = processVote(vote.ip_address, vote.user_id, now);
            if (alert) {
              console.warn('ALERT:', alert.message);
              await producer.send({
                topic: 'security_alerts',
                messages: [{ value: JSON.stringify(alert) }]
              });
            }
          }
        },
      });
      console.log('Kafka consumer reconnected');
    } catch (reconnectErr) {
      console.error('Kafka reconnect failed:', reconnectErr.message);
    }
  });

  producer.on('producer.disconnect', async () => {
    console.error('Kafka producer disconnected, reconnecting...');
    try {
      producer = kafka.producer();
      await producer.connect();
      console.log('Kafka producer reconnected');
    } catch (err) {
      console.error('Kafka producer reconnect failed:', err.message);
    }
  });
}

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, shutting down gracefully...`);
  if (cleanupInterval) clearInterval(cleanupInterval);

  const shutdownTasks = [];

  if (kafkaConsumer) {
    shutdownTasks.push(kafkaConsumer.disconnect().catch(err => console.error('Kafka consumer disconnect error:', err.message)));
  }

  if (producer) {
    shutdownTasks.push(producer.disconnect().catch(err => console.error('Kafka producer disconnect error:', err.message)));
  }

  await Promise.all(shutdownTasks);
  console.log('Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

run().catch(console.error);

module.exports = { processVote, cleanupStaleIPs, ipVotersMap, CLEANUP_INTERVAL_MS, STALE_THRESHOLD_MS };
