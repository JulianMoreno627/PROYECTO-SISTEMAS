const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');

const kafka = new Kafka({
  clientId: 'voter-registration-service',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});

const producer = kafka.producer();
const admin = kafka.admin();

async function run() {
  await admin.connect();
  console.log('Admin connected');

  // Create compacted topic
  await admin.createTopics({
    topics: [{
      topic: 'eligible_voters',
      numPartitions: 1,
      replicationFactor: 1,
      configEntries: [
        { name: 'cleanup.policy', value: 'compact' }
      ]
    }]
  });
  console.log('Topic eligible_voters created (compacted)');
  await admin.disconnect();

  await producer.connect();
  console.log('Producer connected');

  const votersPath = path.join(__dirname, '../../voters.json');
  const voters = JSON.parse(fs.readFileSync(votersPath, 'utf-8'));

  for (const voter of voters) {
    await producer.send({
      topic: 'eligible_voters',
      messages: [
        { key: voter.user_id, value: JSON.stringify(voter) }
      ],
    });
    console.log(`Registered voter: ${voter.user_id}`);
  }

  await producer.disconnect();
  console.log('Voter registration completed');
}

run().catch(console.error);
