import { Kafka } from 'kafkajs';
import amqp from 'amqplib';
import axios from 'axios';
import WebSocket from 'ws';

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const API_URL = process.env.API_URL || 'http://localhost:3000';
const GLOBAL_DASHBOARD_WS = process.env.GLOBAL_DASHBOARD_WS || 'ws://localhost:4000';
const REGIONAL_DASHBOARD_WS = process.env.REGIONAL_DASHBOARD_WS || 'ws://localhost:4001';

export async function createKafkaAdmin() {
  const kafka = new Kafka({ clientId: 'test-admin', brokers: [KAFKA_BROKER] });
  const admin = kafka.admin();
  await admin.connect();
  return admin;
}

export async function createKafkaConsumer(groupId, topic, fromBeginning = true) {
  const kafka = new Kafka({ clientId: `test-consumer-${groupId}`, brokers: [KAFKA_BROKER] });
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning });
  return consumer;
}

export async function createKafkaProducer() {
  const kafka = new Kafka({ clientId: 'test-producer', brokers: [KAFKA_BROKER] });
  const producer = kafka.producer();
  await producer.connect();
  return producer;
}

export async function createRabbitChannel() {
  const conn = await amqp.connect(RABBITMQ_URL);
  const channel = await conn.createChannel();
  return { conn, channel };
}

export async function consumeRabbitMessage(exchange, exchangeType, routingKey = '') {
  const { conn, channel } = await createRabbitChannel();
  await channel.assertExchange(exchange, exchangeType, { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });

  if (routingKey) {
    await channel.bindQueue(q.queue, exchange, routingKey);
  } else {
    await channel.bindQueue(q.queue, exchange, '');
  }

  return new Promise((resolve) => {
    channel.consume(q.queue, (msg) => {
      if (msg) {
        const data = JSON.parse(msg.content.toString());
        const fields = msg.fields;
        conn.close();
        resolve({ data, fields });
      }
    }, { noAck: true });
  });
}

export async function consumeRabbitMessages(exchange, exchangeType, routingKey = '', timeout = 3000) {
  const { conn, channel } = await createRabbitChannel();
  await channel.assertExchange(exchange, exchangeType, { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });

  if (routingKey) {
    await channel.bindQueue(q.queue, exchange, routingKey);
  } else {
    await channel.bindQueue(q.queue, exchange, '');
  }

  const messages = [];
  let timer;

  return new Promise((resolve) => {
    channel.consume(q.queue, (msg) => {
      if (msg) {
        messages.push({
          data: JSON.parse(msg.content.toString()),
          fields: msg.fields,
        });
        clearTimeout(timer);
        timer = setTimeout(() => {
          conn.close();
          resolve(messages);
        }, timeout);
      }
    }, { noAck: true });

    timer = setTimeout(() => {
      conn.close();
      resolve(messages);
    }, timeout);
  });
}

export async function submitVote(vote) {
  return axios.post(`${API_URL}/vote`, vote);
}

export async function connectWebSocket(url) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    ws.on('open', () => resolve(ws));
    ws.on('error', reject);
  });
}

export async function waitForWebSocketMessage(ws, timeout = 10000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('WebSocket message timeout')), timeout);
    ws.once('message', (data) => {
      clearTimeout(timer);
      resolve(JSON.parse(data.toString()));
    });
  });
}

export function closeWebSocket(ws) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.close();
  }
}

export async function waitForService(url, maxRetries = 30, delay = 2000) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const response = await axios.get(url, { timeout: 3000 });
      if (response.status === 200) return true;
    } catch {
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  throw new Error(`Service at ${url} did not become ready`);
}

export async function deleteTopic(admin, topic) {
  try {
    await admin.deleteTopicRecords({ topic, partitionsTimestamps: [{ partition: 0, timestamp: -1 }] });
  } catch {
    // Topic might not exist or have no records
  }
}

export async function resetConsumerOffset(admin, groupId, topic) {
  try {
    await admin.resetOffsets({ groupId, topic });
  } catch {
    // Group might not exist
  }
}
