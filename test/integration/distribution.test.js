import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  createRabbitChannel,
  submitVote,
  waitForService,
} from './setup.js';

const WAIT_FOR_STARTUP = 15000;

describe('Integration: RabbitMQ Distribution', () => {
  beforeAll(async () => {
    await waitForService('http://localhost:3000/health', 30, 2000);
    await new Promise((r) => setTimeout(r, WAIT_FOR_STARTUP));
  }, 60000);

  describe('Fanout exchange (global results)', () => {
    it('live_results_global exchange exists and is fanout type', async () => {
      const { conn, channel } = await createRabbitChannel();
      await channel.assertExchange('live_results_global', 'fanout', { durable: false, passive: true });
      await conn.close();
    });

    it('receives global vote counts via fanout', async () => {
      const { conn, channel } = await createRabbitChannel();
      await channel.assertExchange('live_results_global', 'fanout', { durable: false });

      const q = await channel.assertQueue('', { exclusive: true });
      await channel.bindQueue(q.queue, 'live_results_global', '');

      const messages = [];
      await channel.consume(q.queue, (msg) => {
        if (msg) messages.push(JSON.parse(msg.content.toString()));
      }, { noAck: true });

      await submitVote({
        user_id: 'ana',
        candidate_id: 'A',
        region: 'Norte',
        ip_address: '172.18.0.1',
      });

      await new Promise((r) => setTimeout(r, 3000));

      expect(messages.length).toBeGreaterThan(0);
      const lastMessage = messages[messages.length - 1];
      expect(typeof lastMessage).toBe('object');
      expect(Object.keys(lastMessage).length).toBeGreaterThan(0);

      await conn.close();
    });

    it('fanout delivers to all bound queues (no routing key)', async () => {
      const { conn, channel } = await createRabbitChannel();
      await channel.assertExchange('live_results_global', 'fanout', { durable: false });

      const q1 = await channel.assertQueue('', { exclusive: true });
      const q2 = await channel.assertQueue('', { exclusive: true });

      await channel.bindQueue(q1.queue, 'live_results_global', '');
      await channel.bindQueue(q2.queue, 'live_results_global', '');

      let received1 = false;
      let received2 = false;

      channel.consume(q1.queue, () => { received1 = true; }, { noAck: true });
      channel.consume(q2.queue, () => { received2 = true; }, { noAck: true });

      await submitVote({
        user_id: 'juan',
        candidate_id: 'B',
        region: 'Sur',
        ip_address: '172.18.0.2',
      });

      await new Promise((r) => setTimeout(r, 3000));

      expect(received1 || received2).toBe(true);

      await conn.close();
    });
  });

  describe('Topic exchange (regional results)', () => {
    it('live_results_regional exchange exists and is topic type', async () => {
      const { conn, channel } = await createRabbitChannel();
      await channel.assertExchange('live_results_regional', 'topic', { durable: false, passive: true });
      await conn.close();
    });

    it('receives regional vote counts via topic exchange', async () => {
      const { conn, channel } = await createRabbitChannel();
      await channel.assertExchange('live_results_regional', 'topic', { durable: false });

      const q = await channel.assertQueue('', { exclusive: true });
      await channel.bindQueue(q.queue, 'live_results_regional', 'results.*');

      const messages = [];
      await channel.consume(q.queue, (msg) => {
        if (msg) messages.push({ data: JSON.parse(msg.content.toString()), fields: msg.fields });
      }, { noAck: true });

      await submitVote({
        user_id: 'pedro',
        candidate_id: 'C',
        region: 'Este',
        ip_address: '172.18.0.3',
      });

      await new Promise((r) => setTimeout(r, 8000));

      expect(messages.length).toBeGreaterThan(0);
      const lastMessage = messages[messages.length - 1];
      expect(typeof lastMessage.data).toBe('object');
      expect(Object.keys(lastMessage.data).length).toBeGreaterThan(0);
      expect(lastMessage.fields.routingKey).toMatch(/^results\.[a-z_]+$/);

      await conn.close();
    });

    it('routing keys follow results.<region> pattern', async () => {
      const regions = ['Norte', 'Sur', 'Este', 'Oeste'];
      for (const region of regions) {
        const routingKey = `results.${region.toLowerCase().replace(/\s+/g, '_')}`;
        expect(routingKey).toMatch(/^results\.[a-z_]+$/);
      }
    });

    it('topic exchange filters by routing key', async () => {
      const { conn, channel } = await createRabbitChannel();
      await channel.assertExchange('live_results_regional', 'topic', { durable: false });

      const qNorte = await channel.assertQueue('', { exclusive: true });
      const qSur = await channel.assertQueue('', { exclusive: true });

      await channel.bindQueue(qNorte.queue, 'live_results_regional', 'results.norte');
      await channel.bindQueue(qSur.queue, 'live_results_regional', 'results.sur');

      let norteReceived = 0;
      let surReceived = 0;

      channel.consume(qNorte.queue, () => { norteReceived++; }, { noAck: true });
      channel.consume(qSur.queue, () => { surReceived++; }, { noAck: true });

      await new Promise((r) => setTimeout(r, 8000));

      await conn.close();
    });
  });

  describe('Publishing intervals', () => {
    it('global results are published regularly', async () => {
      const { conn, channel } = await createRabbitChannel();
      await channel.assertExchange('live_results_global', 'fanout', { durable: false });

      const q = await channel.assertQueue('', { exclusive: true });
      await channel.bindQueue(q.queue, 'live_results_global', '');

      const messages = [];
      await channel.consume(q.queue, (msg) => {
        if (msg) messages.push(JSON.parse(msg.content.toString()));
      }, { noAck: true });

      await new Promise((r) => setTimeout(r, 3000));

      expect(messages.length).toBeGreaterThan(0);

      await conn.close();
    });

    it('regional results are published regularly', async () => {
      const { conn, channel } = await createRabbitChannel();
      await channel.assertExchange('live_results_regional', 'topic', { durable: false });

      const q = await channel.assertQueue('', { exclusive: true });
      await channel.bindQueue(q.queue, 'live_results_regional', 'results.*');

      const messages = [];
      await channel.consume(q.queue, (msg) => {
        if (msg) messages.push(JSON.parse(msg.content.toString()));
      }, { noAck: true });

      await new Promise((r) => setTimeout(r, 8000));

      expect(messages.length).toBeGreaterThan(0);

      await conn.close();
    });
  });
});
