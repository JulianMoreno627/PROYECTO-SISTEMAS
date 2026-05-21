const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

// Construye una lista de usuarios elegibles en memoria desde Kaf ka (eligible_voters) y responde validaciones por RPC (RabbitMQ).

const kafka = new Kafka({
  clientId: 'user-validation-service',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
  retry: { retries: 5 }
});

// Lista en memoria (materialización estilo KTable)
const validVoters = new Set();
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
let rabbitConnection, rabbitChannel;
let kafkaConsumer;

async function ensureEligibleVotersTopicAndGetEndOffsets() {
  // Asegura que el topic exista/sea compactado y obtiene los end offsets (para iniciar RPC solo cuando ya cargó el estado inicial).
  const admin = kafka.admin();
  await admin.connect();
  const existingTopics = await admin.listTopics();
  if (!existingTopics.includes('eligible_voters')) {
    await admin.createTopics({
      topics: [
        {
          topic: 'eligible_voters',
          numPartitions: 1,
          replicationFactor: 1,
          configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
        },
      ],
    });
    console.log('Topic eligible_voters created (compacted)');
  }

  const config = await admin.describeConfigs({
    resources: [{ type: 2, name: 'eligible_voters' }],
  });
  const cleanupPolicy = config.resources[0].configEntries.find((c) => c.configName === 'cleanup.policy')?.configValue;
  if (cleanupPolicy !== 'compact') {
    await admin.alterConfigs({
      validateOnly: false,
      resources: [
        { type: 2, name: 'eligible_voters', configEntries: [{ name: 'cleanup.policy', value: 'compact' }] },
      ],
    });
    console.log('Topic eligible_voters updated to compact cleanup policy');
  }

  const offsets = await admin.fetchTopicOffsets('eligible_voters');
  await admin.disconnect();

  const endOffsets = new Map();
  for (const entry of offsets) {
    const partition = Number(entry.partition);
    const endOffset = Number(entry.high ?? entry.offset ?? 0);
    endOffsets.set(partition, Number.isFinite(endOffset) ? endOffset : 0);
  }
  return endOffsets;
}

async function run() {
  const endOffsets = await ensureEligibleVotersTopicAndGetEndOffsets();

  // 1) Consumidor Kafka: lee eligible_voters desde el inicio para llenar validVoters.
  kafkaConsumer = kafka.consumer({ groupId: 'user-validation-service' });
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: 'eligible_voters', fromBeginning: true });

  console.log('Consuming eligible_voters topic...');

  let messageCount = 0;
  let initialDrainComplete = false;
  let drainResolve;
  const drainPromise = new Promise((resolve) => {
    drainResolve = resolve;
  });
  const consumedOffsets = new Map();

  function maybeResolveDrain() {
    // When we reach the end offset, the initial state is fully loaded.
    if (initialDrainComplete) return;
    for (const [partition, endOffset] of endOffsets) {
      const consumed = consumedOffsets.get(partition) || 0;
      if (consumed < endOffset) {
        return;
      }
    }
    initialDrainComplete = true;
    console.log(`Initial drain complete. Loaded ${messageCount} voters.`);
    drainResolve();
  }

  await kafkaConsumer.run({
    eachMessage: async ({ partition, message }) => {
      const userId = message.key.toString();
      validVoters.add(userId);
      messageCount++;
      consumedOffsets.set(partition, Number(message.offset) + 1);
      maybeResolveDrain();
    },
  });

  maybeResolveDrain();

  // Waits for initial state before serving RPC.
  await drainPromise;
  console.log(`Valid voters loaded: ${validVoters.size}`);

  // 2) RabbitMQ RPC Server: listens on user_validation_queue.
  await connectRabbit();

  // Reconnects to Kafka on crash to keep the in-memory state updated.
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

  // Cola RPC no durable. prefetch(1) procesa 1 solicitud a la vez.
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

    // Respuesta RPC = consulta en la lista en memoria.
    const isValid = validVoters.has(userId);
    const response = isValid ? 'valido' : 'invalido';

    // replyTo/correlationId implementan solicitud/respuesta.
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
  try {
    await connectRabbit();
  } catch (err) {
    console.error('RabbitMQ reconnect failed, retrying in 5s:', err.message);
    setTimeout(reconnectRabbit, 5000);
  }
}

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, shutting down gracefully...`);

  const shutdownTasks = [];

  if (kafkaConsumer) {
    shutdownTasks.push(kafkaConsumer.disconnect().catch(err => console.error('Kafka disconnect error:', err.message)));
  }

  if (rabbitChannel) {
    shutdownTasks.push(rabbitChannel.close().catch(err => console.error('RabbitMQ channel close error:', err.message)));
  }

  if (rabbitConnection) {
    shutdownTasks.push(rabbitConnection.close().catch(err => console.error('RabbitMQ close error:', err.message)));
  }

  await Promise.all(shutdownTasks);
  console.log('Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
