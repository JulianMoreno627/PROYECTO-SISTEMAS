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
    await waitForService('http://localhost:3000', 30, 2000);
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

      const consumer = await createKafkaConsumer('test-bot-detection', 'security_alerts', true);
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
      const consumer = await createKafkaConsumer('test-no-bot', 'security_alerts', true);
      const alerts = [];

      await consumer.run({
        eachMessage: async ({ message }) => {
          alerts.push(JSON.parse(message.value.toString()));
        },
      });

      const users = ['ana', 'juan', 'pedro', 'lucia', 'carlos', 'maria'];
      for (let i = 0; i < users.length; i++) {
        await submitVote({
          user_id: users[i],
          candidate_id: 'A',
          region: 'Norte',
          ip_address: `10.88.${i}.1`,
        });
        await new Promise((r) => setTimeout(r, 500));
      }

      await new Promise((r) => setTimeout(r, 5000));
      await consumer.disconnect();

      const relevantAlerts = alerts.filter((a) =>
        users.some((u) => a.users && a.users.includes(u))
      );
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

      const consumer = await createKafkaConsumer('test-alert-structure', 'security_alerts', true);
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
