const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'analytics-archiver',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
  retry: { retries: 5 }
});

let kafkaConsumer;

async function run() {
  kafkaConsumer = kafka.consumer({ groupId: 'analytics-archiver-group' });
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  console.log('Analytics Archiver started. Monitoring raw_votes...');

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      let vote;
      try {
        vote = JSON.parse(message.value.toString());
      } catch (err) {
        console.error('Failed to parse vote message:', err.message);
        return;
      }
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
          try {
            vote = JSON.parse(message.value.toString());
          } catch (parseErr) {
            console.error('Failed to parse vote message:', parseErr.message);
            return;
          }
          console.log(`[AUDIT] Vote archived: User=${vote.user_id}, Candidate=${vote.candidate_id}, Region=${vote.region}, IP=${vote.ip_address}`);
        },
      });
      console.log('Kafka consumer reconnected');
    } catch (reconnectErr) {
      console.error('Kafka reconnect failed:', reconnectErr.message);
    }
  });
}

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, shutting down gracefully...`);

  if (kafkaConsumer) {
    await kafkaConsumer.disconnect().catch(err => console.error('Kafka disconnect error:', err.message));
  }

  console.log('Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
