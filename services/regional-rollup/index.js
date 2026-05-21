const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

// regional-rollup-service (conteo regional)
// Objetivo: consumir raw_votes (Kafka) y mantener conteos por región.
// Publica cada 5s a RabbitMQ en exchange topic (live_results_regional) usando routing keys:
// - results.norte
// - results.sur
// etc.

const kafka = new Kafka({
  clientId: 'regional-rollup-service',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
  retry: { retries: 5 }
});

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
// Estructura: region -> { candidate_id -> count }
const regionalCounts = {};
// Estado por usuario para reflejar el "último voto" (y moverlo de región/candidato si cambia)
const userVotes = new Map();
let kafkaConsumer, rabbitConnection, rabbitChannel;
let publishInterval;

function processVote(userId, region, candidateId) {
  // Si el usuario ya votó antes, se descuenta su voto anterior del conteo de su región anterior.
  const prevVote = userVotes.get(userId);
  if (prevVote && regionalCounts[prevVote.region]) {
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
  regionalCounts[region][candidateId] = (regionalCounts[region][candidateId] || 0) + 1;
  userVotes.set(userId, { region, candidate_id: candidateId });
}

function getRoutingKey(region) {
  // Normaliza región para la routing key (ej: "Costa Norte" -> "results.costa_norte")
  return `results.${region.toLowerCase().replace(/\s+/g, '_')}`;
}

async function run() {
  // Consumidor independiente (groupId propio) para poder procesar el mismo flujo en paralelo
  kafkaConsumer = kafka.consumer({ groupId: 'regional-rollup-processor' });
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: 'raw_votes', fromBeginning: true });

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      let vote;
      try {
        vote = JSON.parse(message.value.toString());
      } catch (err) {
        console.error('Failed to parse vote message:', err.message);
        return;
      }
      const userId = message.key.toString();
      const region = vote.region;
      const candidate = vote.candidate_id;

      if (!userId || !region || !candidate) {
        console.error('Vote message missing required fields');
        return;
      }

      processVote(userId, region, candidate);
      console.log(`Updated regional counts for ${region}:`, regionalCounts[region]);
    },
  });

  await connectRabbit();

  publishInterval = setInterval(() => {
    if (Object.keys(regionalCounts).length > 0 && rabbitChannel) {
      for (const region in regionalCounts) {
        const routingKey = getRoutingKey(region);
        // Topic exchange: la routing key decide qué consumidores reciben cada mensaje
        rabbitChannel.publish('live_results_regional', routingKey, Buffer.from(JSON.stringify(regionalCounts[region])));
        console.log(`Published regional counts for ${region} to RabbitMQ Topic with key ${routingKey}`);
      }
    }
  }, 5000);

  kafkaConsumer.on('consumer.crash', async (err) => {
    console.error('Kafka consumer crashed:', err.message);
    try {
      kafkaConsumer = kafka.consumer({ groupId: 'regional-rollup-processor' });
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
          const userId = message.key.toString();
          const region = vote.region;
          const candidate = vote.candidate_id;
          if (userId && region && candidate) {
            processVote(userId, region, candidate);
          }
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
  const exchange = 'live_results_regional';

  await rabbitChannel.assertExchange(exchange, 'topic', { durable: false });

  rabbitConnection.on('error', (err) => {
    console.error('RabbitMQ connection error:', err.message);
    reconnectRabbit();
  });

  rabbitConnection.on('close', () => {
    console.warn('RabbitMQ connection closed, reconnecting...');
    reconnectRabbit();
  });

  console.log('RabbitMQ connected');
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
  if (publishInterval) clearInterval(publishInterval);

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

module.exports = { processVote, getRoutingKey, regionalCounts, userVotes };
