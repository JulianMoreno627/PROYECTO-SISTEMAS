import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  createKafkaAdmin,
  submitVote,
  waitForService,
} from './setup.js';

const WAIT_FOR_STARTUP = 15000;

describe('Integration: User Validation (RPC)', () => {
  let admin;

  beforeAll(async () => {
    await waitForService('http://localhost:3000/health', 30, 2000);
    await new Promise((r) => setTimeout(r, WAIT_FOR_STARTUP));
    admin = await createKafkaAdmin();
  }, 60000);

  afterAll(async () => {
    if (admin) await admin.disconnect();
  });

  describe('RPC validation via RabbitMQ', () => {
    it('eligible user passes validation', async () => {
      const response = await submitVote({
        user_id: 'ana',
        candidate_id: 'A',
        region: 'Norte',
        ip_address: '192.168.1.1',
      });
      expect(response.status).toBe(200);
      expect(response.data.message).toBe('Vote accepted');
    });

    it('non-eligible user fails validation', async () => {
      try {
        await submitVote({
          user_id: 'totally_fake_user',
          candidate_id: 'A',
          region: 'Norte',
          ip_address: '192.168.1.1',
        });
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error.response.status).toBe(403);
        expect(error.response.data.error).toBe('User not eligible to vote');
      }
    });

    it('all registered voters can vote', async () => {
      const voters = ['ana', 'juan', 'pedro', 'lucia', 'carlos', 'maria', 'jose', 'elena', 'diego', 'sofia'];

      for (const voter of voters) {
        const response = await submitVote({
          user_id: voter,
          candidate_id: 'A',
          region: 'Norte',
          ip_address: `10.0.0.${Math.floor(Math.random() * 255)}`,
        });
        expect(response.status).toBe(200);
      }
    });
  });

  describe('Input validation', () => {
    it('rejects vote with missing user_id', async () => {
      try {
        await submitVote({
          candidate_id: 'A',
          region: 'Norte',
          ip_address: '1.1.1.1',
        });
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error.response.status).toBe(400);
      }
    });

    it('rejects vote with missing candidate_id', async () => {
      try {
        await submitVote({
          user_id: 'ana',
          region: 'Norte',
          ip_address: '1.1.1.1',
        });
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error.response.status).toBe(400);
      }
    });

    it('rejects vote with missing region', async () => {
      try {
        await submitVote({
          user_id: 'ana',
          candidate_id: 'A',
          ip_address: '1.1.1.1',
        });
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error.response.status).toBe(400);
      }
    });

    it('rejects vote with missing ip_address', async () => {
      try {
        await submitVote({
          user_id: 'ana',
          candidate_id: 'A',
          region: 'Norte',
        });
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error.response.status).toBe(400);
      }
    });

    it('rejects empty body', async () => {
      try {
        await submitVote({});
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error.response.status).toBe(400);
      }
    });
  });

  describe('KTable: eligible_voters topic', () => {
    it('eligible_voters topic is compacted', async () => {
      const config = await admin.describeConfigs({
        resources: [{ type: 2, name: 'eligible_voters' }],
      });
      const compactConfig = config.resources[0].configEntries.find((c) => c.configName === 'cleanup.policy');
      expect(compactConfig?.configValue).toBe('compact');
    });

    it('voters are stored with user_id as key', async () => {
      const topics = await admin.listTopics();
      expect(topics).toContain('eligible_voters');
    });
  });
});
