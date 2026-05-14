const axios = require('axios');

const candidates = ['A', 'B', 'C'];
const regions = ['Norte', 'Sur', 'Este', 'Oeste'];
const users = ['ana', 'juan', 'pedro', 'lucia', 'carlos', 'maria', 'jose', 'elena', 'diego', 'sofia', 'unknown'];

async function sendVote() {
  const user = users[Math.floor(Math.random() * users.length)];
  const candidate = candidates[Math.floor(Math.random() * candidates.length)];
  const region = regions[Math.floor(Math.random() * regions.length)];
  
  try {
    console.log(`Sending vote: User=${user}, Candidate=${candidate}, Region=${region}`);
    const response = await axios.post('http://localhost:3000/vote', {
      user_id: user,
      candidate_id: candidate,
      region: region,
      ip_address: '127.0.0.1'
    });
    console.log(`Response: ${response.status} - ${response.data.message || response.data.error}`);
  } catch (error) {
    if (error.response) {
      console.log(`Response: ${error.response.status} - ${error.response.data.error}`);
    } else {
      console.error(`Error sending vote: ${error.message}`);
    }
  }
}

// Send a vote every 2 seconds
setInterval(sendVote, 2000);
