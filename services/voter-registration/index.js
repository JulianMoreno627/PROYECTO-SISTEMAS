// Importaciones
const { Kafka } = require('kafkajs'); 
const fs = require('fs'); 
const path = require('path'); 

const kafka = new Kafka({  // Configuración de Kafka
   clientId: 'voter-registration-service', // Identificador único del cliente
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'] // brokers de Kafka
});

const producer = kafka.producer(); // Producer para enviar mensajes a Kafka
const admin = kafka.admin(); // Admin para administrar topics y configuraciones

async function run() { // Función principal
  await admin.connect(); // Conecta al Admin de Kafka
  console.log('Admin connected'); 


  const existingTopics = await admin.listTopics(); // Lista todos los topics existentes
  if (!existingTopics.includes('eligible_voters')) { // Si el topic no existe
    await admin.createTopics({ // Crea el topic si no existe
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
  } else {
    console.log('Topic eligible_voters already exists');
  }

  const config = await admin.describeConfigs({ // Obtiene la configuración del topic
    resources: [{ type: 2, name: 'eligible_voters' }], // Tipo 2 representa topic
  });
  const cleanupPolicy = config.resources[0].configEntries.find((c) => c.configName === 'cleanup.policy')?.configValue; // Obtiene la configuración de cleanup.policy
  console.log(`Current cleanup policy: ${cleanupPolicy}`);
  
  if (cleanupPolicy !== 'compact') { // Si la configuración de cleanup.policy no es compact
    await admin.alterConfigs({ // Altera la configuración del topic si es necesario
      validateOnly: false, // Valida la configuración sin aplicarla
      resources: [ 
        { type: 2, name: 'eligible_voters', configEntries: [{ name: 'cleanup.policy', value: 'compact' }] }, // Altera la configuración de cleanup.policy a compact
      ],
    });
    console.log('Topic eligible_voters updated to compact cleanup policy');   // console.log = Imprime que el topic ha sido actualizado 
  }
  await admin.disconnect(); // Desconecta del Admin de Kafka

  await producer.connect();
  console.log('Producer connected');

  const votersPath = path.join(__dirname, '../../voters.json'); 
  const voters = JSON.parse(fs.readFileSync(votersPath, 'utf-8')); // Lee el archivo JSON y lo convierte en un objeto JavaScript

  for (const voter of voters) {
    await producer.send({
      topic: 'eligible_voters',
      messages: [
        { key: voter.user_id, value: JSON.stringify(voter) } // Envía el mensaje con el usuario como clave y el JSON como valor
      ],
    });
    console.log(`Registered voter: ${voter.user_id}`); 
  }

  await producer.disconnect(); // Desconecta del Producer de Kafka
  console.log('Voter registration completed');
}

run().catch(console.error); // imprime cualquier error 
