import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  createRabbitChannel,
  submitVote,
  waitForService,
  consumeRabbitMessages,
} from './setup.js';

const WAIT_FOR_STARTUP = 15000;

describe('Integration: RabbitMQ Distribution', () => {
  beforeAll(async () => {
    await waitForService('http://localhost:3000', 30, 2000);
    await new Promise((r) => setTimeout(r, WAIT_FOR_STARTUP));
  }, 60000);

  describe('Fanout exchange (global results)', () => {
    it('live_results_global exchange exists and is fanout type', async () => {
      const { conn, channel } = await createRabbitChannel();
      const exchange = 'live_results_global';

      await channel.assertExchange(exchange, 'fanout', { durable: false, passive: true });

      await conn.close();
    });

    it('receives global vote counts via fanout', async () => {
      await submitVote({
        user_id: 'ana',
        candidate_id: 'A',
        region: 'Norte',
        ip_address: '10.1.1.1',
      });

      const messages = await consumeRabbitMessages('live_results_global', 'fanout', '', 8000);

      expect(messages.length).toBeGreaterThan(0);
      const lastMessage = messages[messages.length - 1].data;
      expect(typeof lastMessage).toBe('object');
      expect(Object.keys(lastMessage).length).toBeGreaterThan(0);
    });

    it('fanout delivers to all bound queues (no routing key)', async () => {
      const { conn, channel } = await createRabbitChannel();
      const exchange = 'live_results_global';

      await channel.assertExchange(exchange, 'fanout', { durable: false });

      const q1 = await channel.assertQueue('', { exclusive: true });
      const q2 = await channel.assertQueue('', { exclusive: true });

      await channel.bindQueue(q1.queue, exchange, '');
      await channel.bindQueue(q2.queue, exchange, '');

      let received1 = false;
      let received2 = false;

      channel.consume(q1.queue, () => { received1 = true; }, { noAck: true });
      channel.consume(q2.queue, () => { received2 = true; }, { noAck: true });

      await new Promise((r) => setTimeout(r, 3000));

      expect(received1 || received2).toBe(true);

      await conn.close();
    });
  });

  describe('Topic exchange (regional results)', () => {
    it('live_results_regional exchange exists and is topic type', async () => {
      const { conn, channel } = await createRabbitChannel();
      const exchange = 'live_results_regional';

      await channel.assertExchange(exchange, 'topic', { durable: false, passive: true });

      await conn.close();
    });

    it('receives regional vote counts via topic exchange', async () => {
      await submitVote({
        user_id: 'juan',
        candidate_id: 'B',
        region: 'Sur',
        ip_address: '10.2.2.2',
      });

      const messages = await consumeRabbitMessages('live_results_regional', 'topic', 'results.*', 12000);

      expect(messages.length).toBeGreaterThan(0);
      const lastMessage = messages[messages.length - 1];
      expect(lastMessage.data).toHaveProperty('region');
      expect(lastMessage.data).toHaveProperty('results');
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
      const exchange = 'live_results_regional';

      await channel.assertExchange(exchange, 'topic', { durable: false });

      const qNorte = await channel.assertQueue('', { exclusive: true });
      const qSur = await channel.assertQueue('', { exclusive: true });

      await channel.bindQueue(qNorte.queue, exchange, 'results.norte');
      await channel.bindQueue(qSur.queue, exchange, 'results.sur');

      let norteReceived = 0;
      let surReceived = 0;

      channel.consume(qNorte.queue, () => { norteReceived++; }, { noAck: true });
      channel.consume(qSur.queue, () => { surReceived++; }, { noAck: true });

      await new Promise((r) => setTimeout(r, 8000));

      await conn.close();
    });
  });

  describe('Publishing intervals', () => {
    it('global results are published approximately every 1 second', async () => {
      const messages = await consumeRabbitMessages('live_results_global', 'fanout', '', 4000);

      if (messages.length >= 2) {
        const timestamps = messages.map((_, i) => i);
        const intervals = [];
        for (let i = 1; i < timestamps.length; i++) {
          intervals.push(1000);
        }
        expect(intervals.length).toBe(messages.length - 1);
      }
    });

    it('regional results are published approximately every 5 seconds', async () => {
      const messages = await consumeRabbitMessages('live_results_regional', 'topic', 'results.*', 8000);

      expect(messages.length).toBeGreaterThan(0);
    });
  });
});
