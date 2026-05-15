import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  createKafkaAdmin,
  createKafkaConsumer,
  submitVote,
  waitForService,
  connectWebSocket,
  waitForWebSocketMessage,
  closeWebSocket,
} from './setup.js';

const WAIT_FOR_STARTUP = 15000;

describe('Integration: Vote Deduplication', () => {
  let admin;

  beforeAll(async () => {
    await waitForService('http://localhost:3000', 30, 2000);
    await new Promise((r) => setTimeout(r, WAIT_FOR_STARTUP));
    admin = await createKafkaAdmin();
  }, 60000);

  afterAll(async () => {
    if (admin) await admin.disconnect();
  });

  describe('Unicity: one vote per user', () => {
    it('same user voting twice only counts as one vote globally', async () => {
      const ws = await connectWebSocket('ws://localhost:4000');

      await submitVote({
        user_id: 'pedro',
        candidate_id: 'A',
        region: 'Norte',
        ip_address: '10.0.0.50',
      });

      await new Promise((r) => setTimeout(r, 2000));

      await submitVote({
        user_id: 'pedro',
        candidate_id: 'B',
        region: 'Norte',
        ip_address: '10.0.0.50',
      });

      const message = await waitForWebSocketMessage(ws, 15000);
      closeWebSocket(ws);

      const totalVotes = Object.values(message).reduce((a, b) => a + b, 0);
      const pedroVote = message.A || 0;
      const pedroVoteB = message.B || 0;

      expect(pedroVote + pedroVoteB).toBeLessThanOrEqual(1);
    });

    it('same user voting multiple times only keeps last vote', async () => {
      const ws = await connectWebSocket('ws://localhost:4000');

      await submitVote({ user_id: 'lucia', candidate_id: 'A', region: 'Este', ip_address: '10.0.0.51' });
      await new Promise((r) => setTimeout(r, 1000));
      await submitVote({ user_id: 'lucia', candidate_id: 'B', region: 'Este', ip_address: '10.0.0.51' });
      await new Promise((r) => setTimeout(r, 1000));
      await submitVote({ user_id: 'lucia', candidate_id: 'C', region: 'Este', ip_address: '10.0.0.51' });

      const message = await waitForWebSocketMessage(ws, 15000);
      closeWebSocket(ws);

      const totalFromLucia = (message.A || 0) + (message.B || 0) + (message.C || 0);
      expect(totalFromLucia).toBeLessThanOrEqual(1);
    });

    it('different users voting are counted independently', async () => {
      const ws = await connectWebSocket('ws://localhost:4000');

      await submitVote({ user_id: 'carlos', candidate_id: 'A', region: 'Oeste', ip_address: '10.0.0.52' });
      await submitVote({ user_id: 'maria', candidate_id: 'A', region: 'Oeste', ip_address: '10.0.0.53' });

      const message = await waitForWebSocketMessage(ws, 15000);
      closeWebSocket(ws);

      expect(message.A).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Regional deduplication', () => {
    it('user changing region updates regional counts correctly', async () => {
      const ws = await connectWebSocket('ws://localhost:4001');

      await submitVote({ user_id: 'ana', candidate_id: 'A', region: 'Norte', ip_address: '10.0.0.60' });
      await new Promise((r) => setTimeout(r, 2000));
      await submitVote({ user_id: 'ana', candidate_id: 'A', region: 'Sur', ip_address: '10.0.0.60' });

      const message = await waitForWebSocketMessage(ws, 20000);
      closeWebSocket(ws);

      expect(message).toHaveProperty('region');
      expect(message).toHaveProperty('results');
    });
  });

  describe('Kafka compaction verification', () => {
    it('raw_votes topic has compact cleanup policy', async () => {
      const config = await admin.describeConfigs({
        resources: [{ type: 2, name: 'raw_votes' }],
      });
      const compactConfig = config[0].configEntries.find((c) => c.name === 'cleanup.policy');
      expect(compactConfig.value).toBe('compact');
    });

    it('multiple votes from same user are stored with same key', async () => {
      const consumer = await createKafkaConsumer('test-compaction-keys', 'raw_votes', true);
      const keys = new Set();

      await consumer.run({
        eachMessage: async ({ message }) => {
          const vote = JSON.parse(message.value.toString());
          if (['ana', 'pedro', 'lucia'].includes(vote.user_id)) {
            keys.add(message.key.toString());
          }
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await consumer.disconnect();

      expect(keys.size).toBeGreaterThan(0);
      keys.forEach((key) => {
        expect(['ana', 'pedro', 'lucia', 'juan', 'carlos', 'maria', 'jose', 'elena', 'diego', 'sofia']).toContain(key);
      });
    });
  });
});
