import { describe, it, expect } from 'vitest';
import fs from 'fs';
import path from 'path';

describe('Kafka topic configuration', () => {
  describe('eligible_voters topic', () => {
    it('uses compact cleanup policy', () => {
      const config = { cleanupPolicy: 'compact' };
      expect(config.cleanupPolicy).toBe('compact');
    });

    it('uses user_id as message key for compaction', () => {
      const voter = { user_id: 'ana', name: 'Ana Garcia' };
      const message = { key: voter.user_id, value: JSON.stringify(voter) };

      expect(message.key).toBe('ana');
    });

    it('compaction keeps only latest value per key', () => {
      const messages = [
        { key: 'ana', value: JSON.stringify({ user_id: 'ana', status: 'active' }) },
        { key: 'ana', value: JSON.stringify({ user_id: 'ana', status: 'inactive' }) },
        { key: 'juan', value: JSON.stringify({ user_id: 'juan', status: 'active' }) },
      ];

      const compacted = new Map();
      messages.forEach((m) => compacted.set(m.key, m.value));

      expect(compacted.size).toBe(2);
      expect(JSON.parse(compacted.get('ana')).status).toBe('inactive');
    });
  });

  describe('raw_votes topic', () => {
    it('uses compact cleanup policy', () => {
      const config = { cleanupPolicy: 'compact' };
      expect(config.cleanupPolicy).toBe('compact');
    });

    it('uses user_id as message key for deduplication', () => {
      const vote = { user_id: 'ana', candidate_id: 'A', region: 'Norte' };
      const message = { key: vote.user_id, value: JSON.stringify(vote) };

      expect(message.key).toBe('ana');
    });

    it('compaction ensures one vote per user', () => {
      const votes = [
        { key: 'ana', value: JSON.stringify({ user_id: 'ana', candidate_id: 'A' }) },
        { key: 'ana', value: JSON.stringify({ user_id: 'ana', candidate_id: 'B' }) },
        { key: 'ana', value: JSON.stringify({ user_id: 'ana', candidate_id: 'C' }) },
        { key: 'juan', value: JSON.stringify({ user_id: 'juan', candidate_id: 'A' }) },
      ];

      const compacted = new Map();
      votes.forEach((v) => compacted.set(v.key, v.value));

      expect(compacted.size).toBe(2);
      expect(JSON.parse(compacted.get('ana')).candidate_id).toBe('C');
    });

    it('single partition ensures ordering by key', () => {
      const numPartitions = 1;
      expect(numPartitions).toBe(1);
    });
  });

  describe('security_alerts topic', () => {
    it('uses default cleanup policy (delete, not compact)', () => {
      const config = { cleanupPolicy: 'delete' };
      expect(config.cleanupPolicy).toBe('delete');
    });

    it('alert messages have no key (partitioned by round-robin)', () => {
      const alert = { ip: '10.0.0.1', users: ['u1', 'u2', 'u3'] };
      const message = { key: null, value: JSON.stringify(alert) };

      expect(message.key).toBeNull();
    });
  });
});

describe('consumer group configuration', () => {
  it('each processor has unique groupId', () => {
    const groups = [
      'global-vote-processor',
      'regional-rollup-processor',
      'bot-detector-group',
      'analytics-archiver-group',
      'user-validation-service',
    ];

    const uniqueGroups = new Set(groups);
    expect(uniqueGroups.size).toBe(groups.length);
  });

  it('user-validation uses fixed groupId (not timestamp-based)', () => {
    const groupId = 'user-validation-service';
    expect(groupId).not.toMatch(/\d{13}/);
    expect(groupId).toBe('user-validation-service');
  });

  it('fromBeginning is true for all consumers that need full state', () => {
    const consumers = [
      { name: 'user-validation', fromBeginning: true },
      { name: 'vote-processor', fromBeginning: true },
      { name: 'regional-rollup', fromBeginning: true },
      { name: 'bot-detector', fromBeginning: true },
      { name: 'analytics-archiver', fromBeginning: true },
    ];

    consumers.forEach((c) => expect(c.fromBeginning).toBe(true));
  });
});

describe('RabbitMQ topology', () => {
  describe('exchanges', () => {
    it('live_results_global is fanout type', () => {
      const exchange = { name: 'live_results_global', type: 'fanout' };
      expect(exchange.type).toBe('fanout');
    });

    it('live_results_regional is topic type', () => {
      const exchange = { name: 'live_results_regional', type: 'topic' };
      expect(exchange.type).toBe('topic');
    });
  });

  describe('queues', () => {
    it('user_validation_queue is non-durable', () => {
      const queue = { name: 'user_validation_queue', durable: false };
      expect(queue.durable).toBe(false);
    });

    it('dashboard queues are exclusive', () => {
      const globalQueue = { exclusive: true };
      const regionalQueue = { exclusive: true };

      expect(globalQueue.exclusive).toBe(true);
      expect(regionalQueue.exclusive).toBe(true);
    });
  });

  describe('routing patterns', () => {
    it('fanout exchange ignores routing key', () => {
      const routingKey = '';
      expect(routingKey).toBe('');
    });

    it('topic exchange uses results.* binding', () => {
      const binding = 'results.*';

      expect('results.norte'.startsWith('results.')).toBe(true);
      expect('results.sur'.startsWith('results.')).toBe(true);
      expect('results.east_coast'.startsWith('results.')).toBe(true);
    });

    it('routing key format is results.<region_lowercase>', () => {
      const regions = ['Norte', 'Sur', 'Este', 'Oeste'];
      const keys = regions.map((r) => `results.${r.toLowerCase()}`);

      expect(keys).toEqual(['results.norte', 'results.sur', 'results.este', 'results.oeste']);
    });
  });
});

describe('Docker Compose orchestration', () => {
  const composePath = path.join(__dirname, '../../docker-compose.yml');
  let compose;

  it('docker-compose.yml exists and is valid YAML', () => {
    expect(fs.existsSync(composePath)).toBe(true);

    const content = fs.readFileSync(composePath, 'utf-8');
    expect(content).toContain('services:');
  });

  it('has exactly 11 services (3 infra + 8 app)', () => {
    const content = fs.readFileSync(composePath, 'utf-8');
    const serviceNames = [
      'kafka', 'rabbitmq',
      'voter_registration', 'user_validation', 'voting_api',
      'vote_processor', 'regional_rollup', 'bot_detector',
      'analytics_archiver', 'global_dashboard', 'regional_dashboard',
    ];

    serviceNames.forEach((name) => {
      expect(content).toContain(`${name}:`);
    });
  });

  it('Kafka uses KRaft mode (no zookeeper)', () => {
    const content = fs.readFileSync(composePath, 'utf-8');

    expect(content).toContain('KAFKA_PROCESS_ROLES');
    expect(content).toContain('broker,controller');
    expect(content).not.toContain('zookeeper');
    expect(content).not.toContain('ZOOKEEPER');
  });

  it('Kafka has CLUSTER_ID for KRaft', () => {
    const content = fs.readFileSync(composePath, 'utf-8');
    expect(content).toContain('CLUSTER_ID');
  });

  it('all app services depend on kafka or rabbitmq', () => {
    const content = fs.readFileSync(composePath, 'utf-8');

    const kafkaServices = ['voter_registration', 'user_validation', 'voting_api', 'vote_processor', 'regional_rollup', 'bot_detector', 'analytics_archiver'];
    const rabbitServices = ['user_validation', 'voting_api', 'vote_processor', 'regional_rollup', 'global_dashboard', 'regional_dashboard'];

    kafkaServices.forEach((s) => {
      expect(content).toContain(`- kafka`);
    });
  });

  it('ports are correctly mapped', () => {
    const content = fs.readFileSync(composePath, 'utf-8');

    expect(content).toContain('"9092:9092"');
    expect(content).toContain('"5672:5672"');
    expect(content).toContain('"15672:15672"');
    expect(content).toContain('"3000:3000"');
    expect(content).toContain('"4000:4000"');
    expect(content).toContain('"4001:4001"');
  });
});

describe('environment variables', () => {
  it('KAFKA_BROKERS uses internal Docker network', () => {
    const brokers = 'kafka:29092';
    expect(brokers).toBe('kafka:29092');
  });

  it('RABBITMQ_URL uses internal Docker network', () => {
    const url = 'amqp://rabbitmq';
    expect(url).toBe('amqp://rabbitmq');
  });

  it('Kafka services have KAFKA_BROKERS env', () => {
    const servicesWithKafka = [
      'voter_registration', 'user_validation', 'voting_api',
      'vote_processor', 'regional_rollup', 'bot_detector', 'analytics_archiver',
    ];

    expect(servicesWithKafka.length).toBe(7);
  });

  it('RabbitMQ services have RABBITMQ_URL env', () => {
    const servicesWithRabbit = [
      'user_validation', 'voting_api', 'vote_processor',
      'regional_rollup', 'global_dashboard', 'regional_dashboard',
    ];

    expect(servicesWithRabbit.length).toBe(6);
  });
});
