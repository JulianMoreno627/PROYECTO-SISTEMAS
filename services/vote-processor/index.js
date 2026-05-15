const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

const kafka = new Kafka({
  clientId: 'vote-processor',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const globalCounts = {};
const userVotes = new Map(); // userId -> candidate_id

async function run() {
  // Kafka Consumer
  const consumer = kafka.consumer({ groupId: 'global-vote-processor' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const vote = JSON.parse(message.value.toString());
      const userId = message.key.toString();
      const candidate = vote.candidate_id;

      const prevCandidate = userVotes.get(userId);
      if (prevCandidate) {
        globalCounts[prevCandidate]--;
        if (globalCounts[prevCandidate] <= 0) {
          delete globalCounts[prevCandidate];
        }
      }

      globalCounts[candidate] = (globalCounts[candidate] || 0) + 1;
      userVotes.set(userId, candidate);
      console.log(`Updated global counts:`, globalCounts);
    },
  });

  // RabbitMQ Publisher
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();
  const exchange = 'live_results_global';

  await channel.assertExchange(exchange, 'fanout', { durable: false });

  setInterval(() => {
    if (Object.keys(globalCounts).length > 0) {
      channel.publish(exchange, '', Buffer.from(JSON.stringify(globalCounts)));
      console.log('Published global counts to RabbitMQ Fanout');
    }
  }, 1000);
}

run().catch(console.error);
