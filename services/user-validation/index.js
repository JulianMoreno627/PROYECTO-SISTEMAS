const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

const kafka = new Kafka({
  clientId: 'user-validation-service',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});

const validVoters = new Set();
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';

async function run() {
  // 1. Kafka Consumer (KTable pattern)
  const consumer = kafka.consumer({ groupId: 'user-validation-group-' + Date.now() });
  await consumer.connect();
  await consumer.subscribe({ topic: 'eligible_voters', fromBeginning: true });

  console.log('Consuming eligible_voters topic...');
  
  await consumer.run({
    eachMessage: async ({ message }) => {
      const userId = message.key.toString();
      validVoters.add(userId);
      console.log(`Updated validVoters set. Current size: ${validVoters.size}`);
    },
  });

  // 2. RabbitMQ RPC Server
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();
  const queue = 'user_validation_queue';

  await channel.assertQueue(queue, { durable: false });
  channel.prefetch(1);
  console.log('Awaiting RPC requests on user_validation_queue');

  channel.consume(queue, async (msg) => {
    const content = JSON.parse(msg.content.toString());
    const userId = content.user_id;
    console.log(`Received validation request for: ${userId}`);

    const isValid = validVoters.has(userId);
    const response = isValid ? 'valido' : 'invalido';

    channel.sendToQueue(msg.properties.replyTo, Buffer.from(response), {
      correlationId: msg.properties.correlationId
    });

    channel.ack(msg);
    console.log(`Sent response: ${response} for user: ${userId}`);
  });
}

run().catch(console.error);
