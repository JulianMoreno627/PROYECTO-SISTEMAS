const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'analytics-archiver',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
  retry: { retries: 3 }
});

let kafkaConsumer;

async function connectWithRetry(fn, label, delay = 3000) {
  while (true) {
    try { await fn(); console.log(`${label} connected`); return; }
    catch (err) { console.error(`${label} failed: ${err.message}. Retrying in ${delay}ms...`); await new Promise((r) => setTimeout(r, delay)); }
  }
}

async function run() {
  kafkaConsumer = kafka.consumer({ groupId: 'analytics-archiver-group' });
  await connectWithRetry(() => kafkaConsumer.connect(), 'Kafka consumer');
  await kafkaConsumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  console.log('Analytics Archiver started. Monitoring raw_votes...');

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      let vote;
      try { vote = JSON.parse(message.value.toString()); }
      catch (err) { console.error('Failed to parse vote message:', err.message); return; }
      console.log(`[AUDIT] Vote archived: User=${vote.user_id}, Candidate=${vote.candidate_id}, Region=${vote.region}, IP=${vote.ip_address}`);
    },
  });

  kafkaConsumer.on('consumer.crash', async (err) => {
    console.error('Kafka consumer crashed:', err.message);
    try {
      kafkaConsumer = kafka.consumer({ groupId: 'analytics-archiver-group' });
      await kafkaConsumer.connect();
      await kafkaConsumer.subscribe({ topic: 'raw_votes', fromBeginning: true });
      await kafkaConsumer.run({
        eachMessage: async ({ message }) => {
          let vote;
          try { vote = JSON.parse(message.value.toString()); } catch { return; }
          console.log(`[AUDIT] Vote archived: User=${vote.user_id}, Candidate=${vote.candidate_id}, Region=${vote.region}, IP=${vote.ip_address}`);
        },
      });
      console.log('Kafka consumer reconnected');
    } catch (e) { console.error('Kafka reconnect failed:', e.message); }
  });
}

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, shutting down...`);
  if (kafkaConsumer) await kafkaConsumer.disconnect().catch(() => {});
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

run().catch(console.error);
