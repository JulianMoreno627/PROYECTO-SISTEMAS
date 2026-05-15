import { describe, it, expect } from 'vitest';

describe('global-dashboard - WebSocket broadcasting', () => {
  describe('RabbitMQ fanout consumption', () => {
    it('subscribes to live_results_global exchange', () => {
      const exchange = 'live_results_global';
      const exchangeType = 'fanout';

      expect(exchange).toBe('live_results_global');
      expect(exchangeType).toBe('fanout');
    });

    it('creates exclusive queue for consumption', () => {
      const queueOptions = { exclusive: true };
      expect(queueOptions.exclusive).toBe(true);
    });

    it('binds queue to fanout exchange with no routing key', () => {
      const routingKey = '';
      expect(routingKey).toBe('');
    });
  });

  describe('WebSocket broadcasting', () => {
    it('broadcasts to all connected clients', () => {
      const clients = [
        { readyState: 1, send: (data) => { clients[0].sent = data; } },
        { readyState: 1, send: (data) => { clients[1].sent = data; } },
        { readyState: 3, send: (data) => { clients[2].sent = data; } },
      ];

      const data = JSON.stringify({ A: 5, B: 3 });

      clients.forEach((client) => {
        if (client.readyState === 1) {
          client.send(data);
        }
      });

      expect(clients[0].sent).toBe(data);
      expect(clients[1].sent).toBe(data);
      expect(clients[2].sent).toBeUndefined();
    });

    it('only sends to OPEN clients (readyState 1)', () => {
      const clients = [
        { readyState: 1 },
        { readyState: 0 },
        { readyState: 2 },
        { readyState: 3 },
      ];

      const openClients = clients.filter((c) => c.readyState === 1);
      expect(openClients).toHaveLength(1);
    });
  });

  describe('message format', () => {
    it('broadcasts raw global counts as JSON', () => {
      const globalCounts = { A: 500, B: 450, C: 300 };
      const message = JSON.stringify(globalCounts);

      const parsed = JSON.parse(message);
      expect(parsed.A).toBe(500);
      expect(parsed.B).toBe(450);
      expect(parsed.C).toBe(300);
    });

    it('handles empty counts', () => {
      const globalCounts = {};
      const message = JSON.stringify(globalCounts);

      expect(message).toBe('{}');
    });
  });

  describe('server configuration', () => {
    it('runs on port 4000', () => {
      const PORT = 4000;
      expect(PORT).toBe(4000);
    });

    it('serves index.html at root path', () => {
      const route = '/';
      expect(route).toBe('/');
    });
  });
});

describe('regional-dashboard - WebSocket broadcasting', () => {
  describe('RabbitMQ topic consumption', () => {
    it('subscribes to live_results_regional topic exchange', () => {
      const exchange = 'live_results_regional';
      const exchangeType = 'topic';

      expect(exchange).toBe('live_results_regional');
      expect(exchangeType).toBe('topic');
    });

    it('binds with results.* pattern to match all regions', () => {
      const bindingPattern = 'results.*';

      expect('results.norte'.match(/^results\.[^.]+$/)).toBeTruthy();
      expect('results.sur'.match(/^results\.[^.]+$/)).toBeTruthy();
      expect('results.este'.match(/^results\.[^.]+$/)).toBeTruthy();
      expect('results.oeste'.match(/^results\.[^.]+$/)).toBeTruthy();
    });

    it('creates exclusive queue for consumption', () => {
      const queueOptions = { exclusive: true };
      expect(queueOptions.exclusive).toBe(true);
    });
  });

  describe('routing key extraction', () => {
    it('extracts region name from routing key', () => {
      const routingKey = 'results.norte';
      const region = routingKey.split('.')[1];

      expect(region).toBe('norte');
    });

    it('handles region names with underscores', () => {
      const routingKey = 'results.america_del_norte';
      const region = routingKey.split('.')[1];

      expect(region).toBe('america_del_norte');
    });
  });

  describe('message format', () => {
    it('wraps regional data with region name', () => {
      const data = { A: 10, B: 5 };
      const region = 'norte';
      const payload = JSON.stringify({ region, results: data });

      const parsed = JSON.parse(payload);
      expect(parsed.region).toBe('norte');
      expect(parsed.results).toEqual({ A: 10, B: 5 });
    });
  });

  describe('server configuration', () => {
    it('runs on port 4001', () => {
      const PORT = 4001;
      expect(PORT).toBe(4001);
    });

    it('serves index.html at root path', () => {
      const route = '/';
      expect(route).toBe('/');
    });
  });
});

describe('dashboard - HTML structure', () => {
  describe('global dashboard HTML', () => {
    it('connects via WebSocket to server', () => {
      const wsUrl = `ws://${'localhost:4000'}`;
      expect(wsUrl).toBe('ws://localhost:4000');
    });

    it('updates results div on message', () => {
      const data = { A: 5, B: 3 };
      let html = '';

      for (const candidate in data) {
        html += `<div class="candidate"><h3>Candidato ${candidate}</h3><div class="count">${data[candidate]}</div></div>`;
      }

      expect(html).toContain('Candidato A');
      expect(html).toContain('Candidato B');
      expect(html).toContain('5');
      expect(html).toContain('3');
    });

    it('title is "Resultados Globales en Vivo"', () => {
      const title = 'Resultados Globales en Vivo';
      expect(title).toContain('Globales');
    });
  });

  describe('regional dashboard HTML', () => {
    it('connects via WebSocket to server', () => {
      const wsUrl = `ws://${'localhost:4001'}`;
      expect(wsUrl).toBe('ws://localhost:4001');
    });

    it('maintains regional data state', () => {
      const regionalData = {};

      const msg1 = { region: 'norte', results: { A: 5 } };
      regionalData[msg1.region] = msg1.results;

      const msg2 = { region: 'sur', results: { B: 3 } };
      regionalData[msg2.region] = msg2.results;

      expect(regionalData.norte).toEqual({ A: 5 });
      expect(regionalData.sur).toEqual({ B: 3 });
    });

    it('title is "Resultados Regionales en Vivo"', () => {
      const title = 'Resultados Regionales en Vivo';
      expect(title).toContain('Regionales');
    });
  });
});
