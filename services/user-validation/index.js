const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

const kafka = new Kafka({
  clientId: 'user-validation-service',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
  retry: { retries: 3 }
});

const validVoters = new Set();
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
let rabbitConnection, rabbitChannel;
let kafkaConsumer;

async function connectWithRetry(fn, label, delay = 3000) {
  while (true) {
    try {
      await fn();
      console.log(`${label} connected`);
      return;
    } catch (err) {
      console.error(`${label} failed: ${err.message}. Retrying in ${delay}ms...`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
}

async function run() {
  kafkaConsumer = kafka.consumer({ groupId: 'user-validation-service' });
  await connectWithRetry(() => kafkaConsumer.connect(), 'Kafka consumer');
  await kafkaConsumer.subscribe({ topic: 'eligible_voters', fromBeginning: true });

  console.log('Consuming eligible_voters topic...');

  let initialDrainComplete = false;
  let drainResolve;
  const drainPromise = new Promise((resolve) => { drainResolve = resolve; });

  let idleTimer;
  let messageCount = 0;

  function resetIdleTimer() {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      if (!initialDrainComplete) {
        initialDrainComplete = true;
        console.log(`Initial drain complete. Loaded ${messageCount} voters.`);
        drainResolve();
      }
    }, 1000);
  }

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      const userId = message.key.toString();
      validVoters.add(userId);
      messageCount++;
      resetIdleTimer();
    },
  });

  resetIdleTimer();
  await drainPromise;
  console.log(`Valid voters loaded: ${validVoters.size}`);

  await connectRabbit();

  kafkaConsumer.on('consumer.crash', async (err) => {
    console.error('Kafka consumer crashed:', err.message);
    try {
      kafkaConsumer = kafka.consumer({ groupId: 'user-validation-service' });
      await kafkaConsumer.connect();
      await kafkaConsumer.subscribe({ topic: 'eligible_voters', fromBeginning: true });
      await kafkaConsumer.run({
        eachMessage: async ({ message }) => {
          const userId = message.key.toString();
          validVoters.add(userId);
        },
      });
      console.log('Kafka consumer reconnected');
    } catch (reconnectErr) {
      console.error('Kafka reconnect failed:', reconnectErr.message);
    }
  });
}

async function connectRabbit() {
  rabbitConnection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await rabbitConnection.createChannel();
  const queue = 'user_validation_queue';

  await rabbitChannel.assertQueue(queue, { durable: false });
  rabbitChannel.prefetch(1);
  console.log('Awaiting RPC requests on user_validation_queue');

  rabbitChannel.consume(queue, async (msg) => {
    if (!msg) return;
    let content;
    try {
      content = JSON.parse(msg.content.toString());
    } catch (err) {
      console.error('Failed to parse RPC message:', err.message);
      rabbitChannel.nack(msg, false, false);
      return;
    }
    const userId = content.user_id;
    console.log(`Received validation request for: ${userId}`);

    const isValid = validVoters.has(userId);
    const response = isValid ? 'valido' : 'invalido';

    rabbitChannel.sendToQueue(msg.properties.replyTo, Buffer.from(response), {
      correlationId: msg.properties.correlationId
    });

    rabbitChannel.ack(msg);
    console.log(`Sent response: ${response} for user: ${userId}`);
  });

  rabbitConnection.on('error', (err) => {
    console.error('RabbitMQ connection error:', err.message);
    reconnectRabbit();
  });

  rabbitConnection.on('close', () => {
    console.warn('RabbitMQ connection closed, reconnecting...');
    reconnectRabbit();
  });
}

async function reconnectRabbit() {
  await connectWithRetry(connectRabbit, 'RabbitMQ reconnect', 5000);
}

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, shutting down gracefully...`);

  const shutdownTasks = [];
  if (kafkaConsumer) shutdownTasks.push(kafkaConsumer.disconnect().catch(() => {}));
  if (rabbitChannel) shutdownTasks.push(rabbitChannel.close().catch(() => {}));
  if (rabbitConnection) shutdownTasks.push(rabbitConnection.close().catch(() => {}));

  await Promise.all(shutdownTasks);
  console.log('Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

run().catch(console.error);
