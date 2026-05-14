const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'analytics-archiver',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});

async function run() {
  const consumer = kafka.consumer({ groupId: 'analytics-archiver-group' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  console.log('Analytics Archiver started. Monitoring raw_votes...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const vote = JSON.parse(message.value.toString());
      console.log(`[AUDIT] Vote archived: User=${vote.user_id}, Candidate=${vote.candidate_id}, Region=${vote.region}, IP=${vote.ip_address}`);
    },
  });
}

run().catch(console.error);
