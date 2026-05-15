const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

const kafka = new Kafka({
  clientId: 'regional-rollup-service',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const regionalCounts = {}; // region -> candidate -> count
const userVotes = new Map(); // userId -> { region, candidate_id }

async function run() {
  const consumer = kafka.consumer({ groupId: 'regional-rollup-processor' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const vote = JSON.parse(message.value.toString());
      const userId = message.key.toString();
      const region = vote.region;
      const candidate = vote.candidate_id;

      const prevVote = userVotes.get(userId);
      if (prevVote) {
        regionalCounts[prevVote.region][prevVote.candidate_id]--;
        if (regionalCounts[prevVote.region][prevVote.candidate_id] <= 0) {
          delete regionalCounts[prevVote.region][prevVote.candidate_id];
        }
        if (Object.keys(regionalCounts[prevVote.region]).length === 0) {
          delete regionalCounts[prevVote.region];
        }
      }

      if (!regionalCounts[region]) {
        regionalCounts[region] = {};
      }
      regionalCounts[region][candidate] = (regionalCounts[region][candidate] || 0) + 1;
      userVotes.set(userId, { region, candidate_id: candidate });
      console.log(`Updated regional counts for ${region}:`, regionalCounts[region]);
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
