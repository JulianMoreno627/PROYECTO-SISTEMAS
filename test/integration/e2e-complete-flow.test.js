import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  createKafkaAdmin,
  createKafkaConsumer,
  createRabbitChannel,
  submitVote,
  waitForService,
  connectWebSocket,
  waitForWebSocketMessage,
  closeWebSocket,
} from './setup.js';

const WAIT_FOR_STARTUP = 15000;

describe('E2E: Complete Voter Flow', () => {
  let admin;

  beforeAll(async () => {
    await waitForService('http://localhost:3000/health', 30, 2000);
    await new Promise((r) => setTimeout(r, WAIT_FOR_STARTUP));
    admin = await createKafkaAdmin();
  }, 60000);

  afterAll(async () => {
    if (admin) await admin.disconnect();
  });

  describe('Full voter journey: HTTP → RPC → Kafka → Processors → Dashboards', () => {
    it('complete flow for a single eligible voter', async () => {
      const testUser = 'ana';
      const testCandidate = 'A';
      const testRegion = 'Norte';
      const testIp = '10.200.0.1';

      // Step 1: Connect WebSocket listeners BEFORE submitting vote
      const globalWs = await connectWebSocket('ws://localhost:4000');
      const regionalWs = await connectWebSocket('ws://localhost:4001');

      const globalMsgPromise = waitForWebSocketMessage(globalWs, 15000);
      const regionalMsgPromise = waitForWebSocketMessage(regionalWs, 20000);

      // Step 2: Submit vote via HTTP (triggers RPC validation internally)
      const response = await submitVote({
        user_id: testUser,
        candidate_id: testCandidate,
        region: testRegion,
        ip_address: testIp,
      });

      // Step 3: Verify HTTP response (means RPC returned "valido" and vote published to Kafka)
      expect(response.status).toBe(200);
      expect(response.data.message).toBe('Vote accepted');

      // Step 4: Verify vote reached Kafka raw_votes topic
      const groupId = `test-e2e-raw-${Date.now()}`;
      const kafkaConsumer = await createKafkaConsumer(groupId, 'raw_votes', true);
      let kafkaVote = null;

      await kafkaConsumer.run({
        eachMessage: async ({ message }) => {
          const vote = JSON.parse(message.value.toString());
          if (vote.user_id === testUser && vote.ip_address === testIp) {
            kafkaVote = vote;
          }
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await kafkaConsumer.disconnect();

      expect(kafkaVote).not.toBeNull();
      expect(kafkaVote.user_id).toBe(testUser);
      expect(kafkaVote.candidate_id).toBe(testCandidate);
      expect(kafkaVote.region).toBe(testRegion);

      // Step 5: Verify global dashboard received the vote count update
      const globalMsg = await globalMsgPromise;
      closeWebSocket(globalWs);

      expect(typeof globalMsg).toBe('object');
      expect(Object.keys(globalMsg).length).toBeGreaterThan(0);
      expect(globalMsg[testCandidate]).toBeGreaterThanOrEqual(1);

      // Step 6: Verify regional dashboard received the regional update
      const regionalMsg = await regionalMsgPromise;
      closeWebSocket(regionalWs);

      expect(regionalMsg).toHaveProperty('region');
      expect(regionalMsg).toHaveProperty('results');
      expect(regionalMsg.region).toBe(testRegion.toLowerCase());
      expect(regionalMsg.results).toHaveProperty(testCandidate);
      expect(regionalMsg.results[testCandidate]).toBeGreaterThanOrEqual(1);

      // Step 7: Verify no false bot alert (single user, single IP)
      const botGroupId = `test-e2e-bot-${Date.now()}`;
      const botConsumer = await createKafkaConsumer(botGroupId, 'security_alerts', true);
      const falseAlerts = [];

      await botConsumer.run({
        eachMessage: async ({ message }) => {
          const alert = JSON.parse(message.value.toString());
          if (alert.ip === testIp) {
            falseAlerts.push(alert);
          }
        },
      });

      await new Promise((r) => setTimeout(r, 3000));
      await botConsumer.disconnect();

      expect(falseAlerts).toHaveLength(0);
    }, 60000);

    it('complete flow for multiple voters across different regions', async () => {
      const voters = [
        { user_id: 'juan', candidate_id: 'B', region: 'Sur', ip: '10.200.1.1' },
        { user_id: 'pedro', candidate_id: 'C', region: 'Este', ip: '10.200.2.1' },
        { user_id: 'lucia', candidate_id: 'A', region: 'Oeste', ip: '10.200.3.1' },
      ];

      const globalWs = await connectWebSocket('ws://localhost:4000');
      const regionalWs = await connectWebSocket('ws://localhost:4001');

      const globalUpdates = [];
      const regionalUpdates = [];

      globalWs.on('message', (data) => {
        globalUpdates.push(JSON.parse(data.toString()));
      });

      regionalWs.on('message', (data) => {
        regionalUpdates.push(JSON.parse(data.toString()));
      });

      // Submit all votes
      for (const voter of voters) {
        const response = await submitVote({
          user_id: voter.user_id,
          candidate_id: voter.candidate_id,
          region: voter.region,
          ip_address: voter.ip,
        });
        expect(response.status).toBe(200);
        await new Promise((r) => setTimeout(r, 1000));
      }

      // Wait for all RabbitMQ updates to propagate
      await new Promise((r) => setTimeout(r, 8000));

      closeWebSocket(globalWs);
      closeWebSocket(regionalWs);

      // Verify global updates were received
      expect(globalUpdates.length).toBeGreaterThan(0);

      // Verify regional updates were received for each region
      const regionsReceived = new Set(regionalUpdates.map((u) => u.region));
      expect(regionsReceived.size).toBeGreaterThanOrEqual(1);
    }, 60000);

    it('ineligible voter is rejected at RPC step (no Kafka publish)', async () => {
      const testIp = '10.200.99.1';

      // Try to vote with non-eligible user
      try {
        await submitVote({
          user_id: 'fake_user_not_in_voters_json',
          candidate_id: 'A',
          region: 'Norte',
          ip_address: testIp,
        });
        expect.fail('Should have been rejected');
      } catch (error) {
        expect(error.response.status).toBe(403);
        expect(error.response.data.error).toBe('User not eligible to vote');
      }

      // Verify no Kafka message was published for this user
      const groupId = `test-e2e-rejected-${Date.now()}`;
      const kafkaConsumer = await createKafkaConsumer(groupId, 'raw_votes', true);
      let foundVote = null;

      await kafkaConsumer.run({
        eachMessage: async ({ message }) => {
          const vote = JSON.parse(message.value.toString());
          if (vote.user_id === 'fake_user_not_in_voters_json') {
            foundVote = vote;
          }
        },
      });

      await new Promise((r) => setTimeout(r, 3000));
      await kafkaConsumer.disconnect();

      expect(foundVote).toBeNull();
    });

    it('user re-voting updates all processors correctly (deduplication)', async () => {
      const testUser = 'carlos';
      const testIp = '10.200.5.1';

      const globalWs = await connectWebSocket('ws://localhost:4000');
      const regionalWs = await connectWebSocket('ws://localhost:4001');

      // First vote: Carlos votes for A in Norte
      await submitVote({
        user_id: testUser,
        candidate_id: 'A',
        region: 'Norte',
        ip_address: testIp,
      });

      await new Promise((r) => setTimeout(r, 3000));

      // Second vote: Carlos changes to B in Sur
      await submitVote({
        user_id: testUser,
        candidate_id: 'B',
        region: 'Sur',
        ip_address: testIp,
      });

      // Wait for processors to update
      await new Promise((r) => setTimeout(r, 8000));

      // Verify Kafka has both votes (before compaction)
      const groupId = `test-e2e-dedup-${Date.now()}`;
      const kafkaConsumer = await createKafkaConsumer(groupId, 'raw_votes', true);
      const carlosVotes = [];

      await kafkaConsumer.run({
        eachMessage: async ({ message }) => {
          const vote = JSON.parse(message.value.toString());
          if (vote.user_id === testUser) {
            carlosVotes.push(vote);
          }
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await kafkaConsumer.disconnect();

      // Kafka received both votes (compaction is async)
      expect(carlosVotes.length).toBeGreaterThanOrEqual(2);

      // Global dashboard should reflect the vote
      const globalMsgPromise = waitForWebSocketMessage(globalWs, 15000);
      const globalMsg = await globalMsgPromise;
      closeWebSocket(globalWs);

      expect(typeof globalMsg).toBe('object');
      expect(Object.keys(globalMsg).length).toBeGreaterThan(0);

      // Regional dashboard should show the updated region
      const regionalMsgPromise = waitForWebSocketMessage(regionalWs, 20000);
      const regionalMsg = await regionalMsgPromise;
      closeWebSocket(regionalWs);

      expect(regionalMsg).toHaveProperty('region');
      expect(regionalMsg).toHaveProperty('results');
    }, 60000);

    it('bot attack triggers security alert', async () => {
      const botIp = '10.200.6.1';
      const botUsers = ['ana', 'juan', 'pedro', 'lucia', 'carlos', 'maria'];

      // Submit votes from 6 different users from the same IP
      for (const user of botUsers) {
        await submitVote({
          user_id: user,
          candidate_id: 'A',
          region: 'Norte',
          ip_address: botIp,
        });
        await new Promise((r) => setTimeout(r, 500));
      }

      // Wait for bot detector to process and publish alert
      await new Promise((r) => setTimeout(r, 3000));

      // Verify security alert was published to Kafka
      const groupId = `test-e2e-bot-${Date.now()}`;
      const alertConsumer = await createKafkaConsumer(groupId, 'security_alerts', true);
      let alert = null;

      await alertConsumer.run({
        eachMessage: async ({ message }) => {
          const data = JSON.parse(message.value.toString());
          if (data.ip === botIp) {
            alert = data;
          }
        },
      });

      await new Promise((r) => setTimeout(r, 5000));
      await alertConsumer.disconnect();

      expect(alert).not.toBeNull();
      expect(alert.ip).toBe(botIp);
      expect(alert.users).toHaveLength(6);
      expect(alert.message).toContain('bot attack');
      expect(alert).toHaveProperty('timestamp');
    }, 30000);

    it('RabbitMQ fanout delivers global results to all subscribers', async () => {
      const { conn, channel } = await createRabbitChannel();
      await channel.assertExchange('live_results_global', 'fanout', { durable: false });

      const q = await channel.assertQueue('', { exclusive: true });
      await channel.bindQueue(q.queue, 'live_results_global', '');

      const messages = [];
      await channel.consume(q.queue, (msg) => {
        if (msg) messages.push({ data: JSON.parse(msg.content.toString()) });
      }, { noAck: true });

      await submitVote({
        user_id: 'elena',
        candidate_id: 'A',
        region: 'Norte',
        ip_address: '10.200.7.1',
      });

      await new Promise((r) => setTimeout(r, 3000));

      expect(messages.length).toBeGreaterThan(0);

      messages.forEach((msg) => {
        expect(typeof msg.data).toBe('object');
        expect(Object.keys(msg.data).length).toBeGreaterThan(0);
        Object.values(msg.data).forEach((count) => {
          expect(typeof count).toBe('number');
          expect(count).toBeGreaterThan(0);
        });
      });

      await conn.close();
    });

    it('RabbitMQ topic exchange delivers regional results with correct routing keys', async () => {
      const { conn, channel } = await createRabbitChannel();
      await channel.assertExchange('live_results_regional', 'topic', { durable: false });

      const q = await channel.assertQueue('', { exclusive: true });
      await channel.bindQueue(q.queue, 'live_results_regional', 'results.*');

      const messages = [];
      await channel.consume(q.queue, (msg) => {
        if (msg) messages.push({ data: JSON.parse(msg.content.toString()), fields: msg.fields });
      }, { noAck: true });

      await submitVote({
        user_id: 'diego',
        candidate_id: 'B',
        region: 'Sur',
        ip_address: '10.200.8.1',
      });

      await new Promise((r) => setTimeout(r, 8000));

      expect(messages.length).toBeGreaterThan(0);

      messages.forEach((msg) => {
        expect(typeof msg.data).toBe('object');
        expect(Object.keys(msg.data).length).toBeGreaterThan(0);
        expect(msg.fields.routingKey).toMatch(/^results\.[a-z_]+$/);
      });

      await conn.close();
    });
  });

  describe('Infrastructure verification', () => {
    it('all required Kafka topics exist', async () => {
      const topics = await admin.listTopics();
      expect(topics).toContain('eligible_voters');
      expect(topics).toContain('raw_votes');
      expect(topics).toContain('security_alerts');
    });

    it('Kafka topics use correct cleanup policies', async () => {
      const eligibleConfig = await admin.describeConfigs({
        resources: [{ type: 2, name: 'eligible_voters' }],
      });
      const eligibleCompact = eligibleConfig.resources[0].configEntries.find((c) => c.configName === 'cleanup.policy');
      expect(eligibleCompact?.configValue).toBe('compact');

      const rawConfig = await admin.describeConfigs({
        resources: [{ type: 2, name: 'raw_votes' }],
      });
      const rawCompact = rawConfig.resources[0].configEntries.find((c) => c.configName === 'cleanup.policy');
      expect(rawCompact?.configValue).toBe('compact');
    });

    it('voters are registered in eligible_voters topic', async () => {
      const groupId = `test-e2e-voters-${Date.now()}`;
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
      expect(voters).toContain('pedro');
    });
  });
});
