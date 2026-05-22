const axios = require('axios');

const API_URL = process.env.API_URL || 'http://localhost:3000';
const INTERVAL_MS = parseInt(process.env.INTERVAL_MS, 10) || 2000;
const MAX_VOTES = parseInt(process.env.MAX_VOTES, 10) || 0; // 0 = unlimited
const BOT_MODE = process.env.BOT_MODE === 'true';

const candidates = ['A', 'B', 'C'];
const regions = ['Norte', 'Sur', 'Este', 'Oeste'];
const users = ['ana', 'juan', 'pedro', 'lucia', 'carlos', 'maria', 'jose', 'elena', 'diego', 'sofia'];

let votesSent = 0;
let votesAccepted = 0;
let votesRejected = 0;
let votesError = 0;

function randomIP() {
  return `${Math.floor(Math.random() * 223) + 1}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`;
}

function getUser() {
  if (Math.random() < 0.15) return 'fake_user_' + Math.floor(Math.random() * 9999);
  return users[Math.floor(Math.random() * users.length)];
}

async function waitForHealth(maxRetries = 30, delay = 2000) {
  console.log('Esperando a que la API de votación esté lista...');
  for (let i = 0; i < maxRetries; i++) {
    try {
      const res = await axios.get(`${API_URL}/health`, { timeout: 3000 });
      if (res.data.status === 'ready') {
        console.log('API de votación lista!\n');
        return true;
      }
      console.log(`  Estado: ${res.data.status} (reintentando...)`);
    } catch {
      console.log(`  API no accesible (reintentando...)`);
    }
    await new Promise((r) => setTimeout(r, delay));
  }
  console.error('La API no se puso lista a tiempo');
  process.exit(1);
}

async function sendVote() {
  const user = getUser();
  const candidate = candidates[Math.floor(Math.random() * candidates.length)];
  const region = regions[Math.floor(Math.random() * regions.length)];
  const ip = BOT_MODE ? '10.0.0.1' : randomIP();

  try {
    const response = await axios.post(`${API_URL}/vote`, {
      user_id: user,
      candidate_id: candidate,
      region: region,
      ip_address: ip,
    }, { timeout: 10000 });

    votesSent++;
    votesAccepted++;
    console.log(`[${votesSent}] ✓ ${user} → ${candidate} (${region}) [${ip}] — ${response.data.message}`);
  } catch (error) {
    votesSent++;
    if (error.response) {
      votesRejected++;
      console.log(`[${votesSent}] ✗ ${user} → ${candidate} (${region}) [${ip}] — ${error.response.status}: ${error.response.data.error}`);
    } else {
      votesError++;
      console.error(`[${votesSent}] ⚠ ${user} — ${error.message}`);
    }
  }
}

async function main() {
  await waitForHealth();

  console.log(`Modo: ${BOT_MODE ? 'BOT (IP fija 10.0.0.1)' : 'Normal (IPs aleatorias)'}`);
  console.log(`Intervalo: ${INTERVAL_MS}ms | Máx votos: ${MAX_VOTES || 'sin límite'}\n`);

  if (MAX_VOTES > 0) {
    for (let i = 0; i < MAX_VOTES; i++) {
      await sendVote();
      if (i < MAX_VOTES - 1) await new Promise((r) => setTimeout(r, INTERVAL_MS));
    }
    console.log(`\n--- Resultados: ${votesAccepted} aceptados, ${votesRejected} rechazados, ${votesError} errores ---`);
    process.exit(0);
  } else {
    setInterval(sendVote, INTERVAL_MS);
  }
}

process.on('SIGINT', () => {
  console.log(`\n\n--- Detenido. Resultados: ${votesAccepted} aceptados, ${votesRejected} rechazados, ${votesError} errores ---`);
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log(`\n\n--- Detenido. Resultados: ${votesAccepted} aceptados, ${votesRejected} rechazados, ${votesError} errores ---`);
  process.exit(0);
});

main().catch(console.error);
