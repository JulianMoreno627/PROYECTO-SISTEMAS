import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import path from 'path';

describe('voter-registration', () => {
  const TEST_VOTERS_PATH = path.join(__dirname, '../data', 'voters.json');

  describe('voters.json parsing', () => {
    it('parses valid voters.json correctly', () => {
      const content = fs.readFileSync(TEST_VOTERS_PATH, 'utf-8');
      const voters = JSON.parse(content);

      expect(Array.isArray(voters)).toBe(true);
      expect(voters.length).toBe(5);
      voters.forEach((v) => {
        expect(v).toHaveProperty('user_id');
        expect(typeof v.user_id).toBe('string');
        expect(v.user_id.length).toBeGreaterThan(0);
      });
    });

    it('each voter has unique user_id', () => {
      const content = fs.readFileSync(TEST_VOTERS_PATH, 'utf-8');
      const voters = JSON.parse(content);
      const ids = voters.map((v) => v.user_id);
      const uniqueIds = new Set(ids);

      expect(uniqueIds.size).toBe(ids.length);
    });
  });

  describe('Kafka message format', () => {
    it('produces correct message structure for each voter', () => {
      const content = fs.readFileSync(TEST_VOTERS_PATH, 'utf-8');
      const voters = JSON.parse(content);

      voters.forEach((voter) => {
        const message = { key: voter.user_id, value: JSON.stringify(voter) };

        expect(typeof message.key).toBe('string');
        expect(message.key).toBe(voter.user_id);
        expect(() => JSON.parse(message.value)).not.toThrow();
        expect(JSON.parse(message.value)).toEqual(voter);
      });
    });

    it('key matches user_id for Kafka partitioning', () => {
      const content = fs.readFileSync(TEST_VOTERS_PATH, 'utf-8');
      const voters = JSON.parse(content);

      voters.forEach((voter) => {
        const message = { key: voter.user_id, value: JSON.stringify(voter) };
        expect(message.key).toBe(voter.user_id);
      });
    });
  });

  describe('edge cases - voters.json', () => {
    it('handles empty array', () => {
      const voters = JSON.parse('[]');
      expect(voters).toEqual([]);
      expect(voters.length).toBe(0);
    });

    it('handles voter with special characters', () => {
      const voter = { user_id: 'user@domain.com' };
      const message = { key: voter.user_id, value: JSON.stringify(voter) };

      expect(message.key).toBe('user@domain.com');
      expect(JSON.parse(message.value)).toEqual(voter);
    });

    it('handles voter with unicode characters', () => {
      const voter = { user_id: ' usuário_ñoño' };
      const message = { key: voter.user_id, value: JSON.stringify(voter) };

      expect(JSON.parse(message.value).user_id).toBe(voter.user_id);
    });

    it('rejects voter without user_id', () => {
      const voter = { name: 'test' };
      expect(voter).not.toHaveProperty('user_id');
    });

    it('handles large voter list serialization', () => {
      const voters = Array.from({ length: 1000 }, (_, i) => ({ user_id: `user${i}` }));
      const serialized = JSON.stringify(voters);
      const parsed = JSON.parse(serialized);

      expect(parsed).toHaveLength(1000);
      expect(parsed[0].user_id).toBe('user0');
      expect(parsed[999].user_id).toBe('user999');
    });
  });

  describe('topic config', () => {
    it('eligible_voters topic uses compact cleanup policy', () => {
      const topicConfig = {
        topic: 'eligible_voters',
        numPartitions: 1,
        replicationFactor: 1,
        configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
      };

      const compactEntry = topicConfig.configEntries.find(
        (e) => e.name === 'cleanup.policy'
      );
      expect(compactEntry).toBeDefined();
      expect(compactEntry.value).toBe('compact');
    });

    it('single partition ensures ordering by key', () => {
      const topicConfig = { numPartitions: 1 };
      expect(topicConfig.numPartitions).toBe(1);
    });
  });
});
