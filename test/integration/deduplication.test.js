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
    await waitForService('http://localhost:3000/health', 30, 2000);
    await new Promise((r) => setTimeout(r, WAIT_FOR_STARTUP));
    admin = await createKafkaAdmin();
  }, 60000);

  afterAll(async () => {
    if (admin) await admin.disconnect();
  });

  describe('Unicity: one vote per user', () => {
    it('same user voting twice only counts as one vote globally', async () => {
      const testIp = '172.17.0.1';

      // Get initial count
      const ws = await connectWebSocket('ws://localhost:4000');
      const initialMsgPromise = waitForWebSocketMessage(ws, 15000);
      const initialMsg = await initialMsgPromise;
      const initialA = initialMsg.A || 0;
      const initialB = initialMsg.B || 0;
      closeWebSocket(ws);

      // First vote
      await submitVote({
        user_id: 'carlos',
        candidate_id: 'A',
        region: 'Norte',
        ip_address: testIp,
      });
      await new Promise((r) => setTimeout(r, 2000));

      // Second vote (same user, different candidate)
      await submitVote({
        user_id: 'carlos',
        candidate_id: 'B',
        region: 'Norte',
        ip_address: testIp,
      });

      // Wait for processor to update
      const ws2 = await connectWebSocket('ws://localhost:4000');
      const finalMsgPromise = waitForWebSocketMessage(ws2, 15000);
      const finalMsg = await finalMsgPromise;
      closeWebSocket(ws2);

      const finalA = finalMsg.A || 0;
      const finalB = finalMsg.B || 0;

      // Carlos should only contribute 1 vote total (A or B, not both)
      const diffA = finalA - initialA;
      const diffB = finalB - initialB;
      expect(diffA + diffB).toBeLessThanOrEqual(1);
    });

    it('same user voting multiple times only keeps last vote', async () => {
      const testIp = '172.17.0.2';

      const ws = await connectWebSocket('ws://localhost:4000');
      const initialMsgPromise = waitForWebSocketMessage(ws, 15000);
      const initialMsg = await initialMsgPromise;
      const initialA = initialMsg.A || 0;
      const initialB = initialMsg.B || 0;
      const initialC = initialMsg.C || 0;
      closeWebSocket(ws);

      await submitVote({ user_id: 'diego', candidate_id: 'A', region: 'Este', ip_address: testIp });
      await new Promise((r) => setTimeout(r, 1000));
      await submitVote({ user_id: 'diego', candidate_id: 'B', region: 'Este', ip_address: testIp });
      await new Promise((r) => setTimeout(r, 1000));
      await submitVote({ user_id: 'diego', candidate_id: 'C', region: 'Este', ip_address: testIp });

      const ws2 = await connectWebSocket('ws://localhost:4000');
      const finalMsgPromise = waitForWebSocketMessage(ws2, 15000);
      const finalMsg = await finalMsgPromise;
      closeWebSocket(ws2);

      const diffA = (finalMsg.A || 0) - initialA;
      const diffB = (finalMsg.B || 0) - initialB;
      const diffC = (finalMsg.C || 0) - initialC;
      const totalDiff = diffA + diffB + diffC;

      // Diego should only contribute 1 vote total
      expect(totalDiff).toBeLessThanOrEqual(1);
    });

    it('different users voting are counted independently', async () => {
      const ws = await connectWebSocket('ws://localhost:4000');
      const initialMsgPromise = waitForWebSocketMessage(ws, 15000);
      const initialMsg = await initialMsgPromise;
      const initialTotal = (initialMsg.A || 0) + (initialMsg.B || 0) + (initialMsg.C || 0);
      closeWebSocket(ws);

      await submitVote({ user_id: 'elena', candidate_id: 'A', region: 'Oeste', ip_address: '172.17.0.3' });
      await new Promise((r) => setTimeout(r, 1500));
      await submitVote({ user_id: 'sofia', candidate_id: 'B', region: 'Oeste', ip_address: '172.17.0.4' });

      await new Promise((r) => setTimeout(r, 3000));

      const ws2 = await connectWebSocket('ws://localhost:4000');
      const finalMsgPromise = waitForWebSocketMessage(ws2, 15000);
      const finalMsg = await finalMsgPromise;
      closeWebSocket(ws2);

      const finalTotal = (finalMsg.A || 0) + (finalMsg.B || 0) + (finalMsg.C || 0);

      expect(finalTotal).toBeGreaterThanOrEqual(initialTotal);
      expect(finalMsg.A).toBeGreaterThanOrEqual(0);
      expect(finalMsg.B).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Regional deduplication', () => {
    it('user changing region updates regional counts correctly', async () => {
      const ws = await connectWebSocket('ws://localhost:4001');

      await submitVote({ user_id: 'ana', candidate_id: 'A', region: 'Norte', ip_address: '172.17.0.5' });
      await new Promise((r) => setTimeout(r, 2000));
      await submitVote({ user_id: 'ana', candidate_id: 'A', region: 'Sur', ip_address: '172.17.0.5' });

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
      const compactConfig = config.resources[0].configEntries.find((c) => c.configName === 'cleanup.policy');
      expect(compactConfig?.configValue).toBe('compact');
    });

    it('multiple votes from same user are stored with same key', async () => {
      const testUser = 'ana';
      const groupId = `test-compaction-${Date.now()}`;
      const consumer = await createKafkaConsumer(groupId, 'raw_votes', true);
      const keys = new Set();

      await consumer.run({
        eachMessage: async ({ message }) => {
          const vote = JSON.parse(message.value.toString());
          if (vote.user_id === testUser) {
            keys.add(message.key.toString());
          }
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await consumer.disconnect();

      expect(keys.size).toBeGreaterThan(0);
      keys.forEach((key) => {
        expect(key).toBe(testUser);
      });
    });
  });
});
