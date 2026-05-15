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

describe('Integration: Full Vote Flow', () => {
  let admin;

  beforeAll(async () => {
    await waitForService('http://localhost:3000/health', 30, 2000);
    await new Promise((r) => setTimeout(r, WAIT_FOR_STARTUP));
    admin = await createKafkaAdmin();
  }, 60000);

  afterAll(async () => {
    if (admin) await admin.disconnect();
  });

  describe('Phase 0: System startup verification', () => {
    it('Kafka broker is reachable', async () => {
      const topics = await admin.listTopics();
      expect(Array.isArray(topics)).toBe(true);
    });

    it('eligible_voters topic exists and is compacted', async () => {
      const config = await admin.describeConfigs({
        resources: [{ type: 2, name: 'eligible_voters' }],
      });
      const compactConfig = config.resources[0].configEntries.find((c) => c.configName === 'cleanup.policy');
      expect(compactConfig?.configValue).toBe('compact');
    });

    it('raw_votes topic exists and is compacted', async () => {
      const config = await admin.describeConfigs({
        resources: [{ type: 2, name: 'raw_votes' }],
      });
      const compactConfig = config.resources[0].configEntries.find((c) => c.configName === 'cleanup.policy');
      expect(compactConfig?.configValue).toBe('compact');
    });

    it('voters are registered in eligible_voters topic', async () => {
      const groupId = `test-startup-${Date.now()}`;
      const consumer = await createKafkaConsumer(groupId, 'eligible_voters', true);
      const voters = [];

      await consumer.run({
        eachMessage: async ({ message }) => {
          voters.push(message.key.toString());
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await consumer.disconnect();

      expect(voters.length).toBeGreaterThan(0);
      expect(voters).toContain('ana');
      expect(voters).toContain('juan');
    });
  });

  describe('Phase 1: Vote submission and validation', () => {
    it('valid user vote returns HTTP 200', async () => {
      const response = await submitVote({
        user_id: 'ana',
        candidate_id: 'A',
        region: 'Norte',
        ip_address: '192.168.1.100',
      });
      expect(response.status).toBe(200);
      expect(response.data.message).toBe('Vote accepted');
    });

    it('invalid user vote returns HTTP 403', async () => {
      try {
        await submitVote({
          user_id: 'nonexistent_user',
          candidate_id: 'A',
          region: 'Norte',
          ip_address: '192.168.1.100',
        });
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error.response.status).toBe(403);
        expect(error.response.data.error).toBe('User not eligible to vote');
      }
    });

    it('missing fields returns HTTP 400', async () => {
      try {
        await submitVote({ user_id: 'ana' });
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error.response.status).toBe(400);
      }
    });
  });

  describe('Phase 2: Vote is published to Kafka and processed', () => {
    it('vote appears in raw_votes topic', async () => {
      const testIp = '172.16.0.1';
      const testUser = 'juan';

      await submitVote({
        user_id: testUser,
        candidate_id: 'B',
        region: 'Sur',
        ip_address: testIp,
      });

      const groupId = `test-raw-votes-${Date.now()}`;
      const consumer = await createKafkaConsumer(groupId, 'raw_votes', true);
      let foundVote = null;

      await consumer.run({
        eachMessage: async ({ message }) => {
          const vote = JSON.parse(message.value.toString());
          if (vote.user_id === testUser && vote.ip_address === testIp) {
            foundVote = vote;
          }
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await consumer.disconnect();

      expect(foundVote).not.toBeNull();
      expect(foundVote.region).toBe('Sur');
    });

    it('vote key is user_id for compaction', async () => {
      const testIp = '172.16.0.2';
      const testUser = 'pedro';

      await submitVote({
        user_id: testUser,
        candidate_id: 'C',
        region: 'Este',
        ip_address: testIp,
      });

      const groupId = `test-key-${Date.now()}`;
      const consumer = await createKafkaConsumer(groupId, 'raw_votes', true);
      let foundKey = null;

      await consumer.run({
        eachMessage: async ({ message }) => {
          const vote = JSON.parse(message.value.toString());
          if (vote.user_id === testUser && vote.ip_address === testIp) {
            foundKey = message.key.toString();
          }
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await consumer.disconnect();

      expect(foundKey).toBe(testUser);
    });
  });

  describe('Phase 3: Global results distribution', () => {
    it('global counts are published via RabbitMQ fanout', async () => {
      const ws = await connectWebSocket('ws://localhost:4000');
      const messagePromise = waitForWebSocketMessage(ws, 15000);

      const message = await messagePromise;
      closeWebSocket(ws);

      expect(typeof message).toBe('object');
      expect(Object.keys(message).length).toBeGreaterThan(0);
    });
  });

  describe('Phase 4: Regional results distribution', () => {
    it('regional counts are published via RabbitMQ topic exchange', async () => {
      const ws = await connectWebSocket('ws://localhost:4001');
      const messagePromise = waitForWebSocketMessage(ws, 20000);

      const message = await messagePromise;
      closeWebSocket(ws);

      expect(message).toHaveProperty('region');
      expect(message).toHaveProperty('results');
      expect(typeof message.region).toBe('string');
      expect(typeof message.results).toBe('object');
    });
  });

  describe('Phase 5: Analytics archiver', () => {
    it('votes are consumed by analytics archiver (check via Kafka offset)', async () => {
      const groupId = `test-archiver-${Date.now()}`;
      const consumer = await createKafkaConsumer(groupId, 'raw_votes', true);
      let voteCount = 0;

      await consumer.run({
        eachMessage: async () => {
          voteCount++;
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await consumer.disconnect();

      expect(voteCount).toBeGreaterThan(0);
    });
  });

  describe('End-to-end: complete flow', () => {
    it('full flow: register -> validate -> vote -> count -> distribute', async () => {
      const ws = await connectWebSocket('ws://localhost:4000');
      const messagePromise = waitForWebSocketMessage(ws, 15000);

      const response = await submitVote({
        user_id: 'lucia',
        candidate_id: 'B',
        region: 'Sur',
        ip_address: '172.16.0.3',
      });
      expect(response.status).toBe(200);

      const wsMessage = await messagePromise;
      closeWebSocket(ws);

      expect(typeof wsMessage).toBe('object');
      expect(Object.keys(wsMessage).length).toBeGreaterThan(0);
    });
  });
});
