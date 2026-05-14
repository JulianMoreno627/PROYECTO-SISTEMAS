const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

const kafka = new Kafka({
  clientId: 'regional-rollup-service',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const regionalCounts = {}; // region -> candidate -> count

async function run() {
  const consumer = kafka.consumer({ groupId: 'regional-rollup-processor' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const vote = JSON.parse(message.value.toString());
      if (!regionalCounts[vote.region]) {
        regionalCounts[vote.region] = {};
      }
      regionalCounts[vote.region][vote.candidate_id] = (regionalCounts[vote.region][vote.candidate_id] || 0) + 1;
      console.log(`Updated regional counts for ${vote.region}:`, regionalCounts[vote.region]);
    },
  });

  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();
  const exchange = 'live_results_regional';

  await channel.assertExchange(exchange, 'topic', { durable: false });

  setInterval(() => {
    for (const region in regionalCounts) {
      const routingKey = `results.${region.toLowerCase().replace(/\s+/g, '_')}`;
      channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(regionalCounts[region])));
      console.log(`Published regional counts for ${region} to RabbitMQ Topic with key ${routingKey}`);
    }
  }, 5000);
}

run().catch(console.error);
