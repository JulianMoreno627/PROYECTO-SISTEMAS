import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  createKafkaAdmin,
  createKafkaConsumer,
  submitVote,
  waitForService,
} from './setup.js';

const WAIT_FOR_STARTUP = 15000;

describe('Integration: Bot Detection', () => {
  let admin;

  beforeAll(async () => {
    await waitForService('http://localhost:3000/health', 30, 2000);
    await new Promise((r) => setTimeout(r, WAIT_FOR_STARTUP));
    admin = await createKafkaAdmin();
  }, 60000);

  afterAll(async () => {
    if (admin) await admin.disconnect();
  });

  describe('Bot attack detection', () => {
    it('detects >5 users voting from same IP', async () => {
      const botIp = '10.99.99.99';
      const users = ['ana', 'juan', 'pedro', 'lucia', 'carlos', 'maria'];

      for (const user of users) {
        await submitVote({
          user_id: user,
          candidate_id: 'A',
          region: 'Norte',
          ip_address: botIp,
        });
        await new Promise((r) => setTimeout(r, 500));
      }

      const groupId = `test-bot-${Date.now()}`;
      const consumer = await createKafkaConsumer(groupId, 'security_alerts', true);
      let alert = null;

      await consumer.run({
        eachMessage: async ({ message }) => {
          const data = JSON.parse(message.value.toString());
          if (data.ip === botIp) {
            alert = data;
          }
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await consumer.disconnect();

      expect(alert).not.toBeNull();
      expect(alert.ip).toBe(botIp);
      expect(alert.users).toHaveLength(6);
      expect(alert.message).toContain('bot attack');
    });

    it('does NOT alert when votes come from different IPs', async () => {
      const uniqueBaseIp = '10.88.88';
      const users = ['ana', 'juan', 'pedro', 'lucia', 'carlos', 'maria'];
      const userIps = {};

      // Submit votes from different IPs
      for (let i = 0; i < users.length; i++) {
        const ip = `${uniqueBaseIp}.${i}`;
        userIps[users[i]] = ip;
        await submitVote({
          user_id: users[i],
          candidate_id: 'A',
          region: 'Norte',
          ip_address: ip,
        });
        await new Promise((r) => setTimeout(r, 500));
      }

      // Check that no alert was generated for any of these IPs
      const groupId = `test-no-bot-${Date.now()}`;
      const consumer = await createKafkaConsumer(groupId, 'security_alerts', true);
      const relevantAlerts = [];

      await consumer.run({
        eachMessage: async ({ message }) => {
          const alert = JSON.parse(message.value.toString());
          if (users.some((u) => alert.users && alert.users.includes(u) && alert.ip === userIps[u])) {
            relevantAlerts.push(alert);
          }
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await consumer.disconnect();

      expect(relevantAlerts).toHaveLength(0);
    });
  });

  describe('Security alerts topic', () => {
    it('security_alerts topic exists', async () => {
      const topics = await admin.listTopics();
      expect(topics).toContain('security_alerts');
    });

    it('alert message has correct structure', async () => {
      const botIp = '10.99.99.98';
      const users = ['ana', 'juan', 'pedro', 'lucia', 'carlos', 'maria'];

      for (const user of users) {
        await submitVote({
          user_id: user,
          candidate_id: 'A',
          region: 'Norte',
          ip_address: botIp,
        });
        await new Promise((r) => setTimeout(r, 300));
      }

      const groupId = `test-alert-structure-${Date.now()}`;
      const consumer = await createKafkaConsumer(groupId, 'security_alerts', true);
      let alert = null;

      await consumer.run({
        eachMessage: async ({ message }) => {
          const data = JSON.parse(message.value.toString());
          if (data.ip === botIp) {
            alert = data;
          }
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await consumer.disconnect();

      expect(alert).not.toBeNull();
      expect(alert).toHaveProperty('ip');
      expect(alert).toHaveProperty('users');
      expect(alert).toHaveProperty('message');
      expect(alert).toHaveProperty('timestamp');
      expect(Array.isArray(alert.users)).toBe(true);
      expect(typeof alert.timestamp).toBe('number');
    });
  });
});
