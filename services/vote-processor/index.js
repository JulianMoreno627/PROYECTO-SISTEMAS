const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

const kafka = new Kafka({
  clientId: 'vote-processor',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const globalCounts = {};

async function run() {
  // Kafka Consumer
  const consumer = kafka.consumer({ groupId: 'global-vote-processor' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const vote = JSON.parse(message.value.toString());
      globalCounts[vote.candidate_id] = (globalCounts[vote.candidate_id] || 0) + 1;
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
