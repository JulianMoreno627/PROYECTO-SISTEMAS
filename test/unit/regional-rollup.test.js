import { describe, it, expect, beforeEach } from 'vitest';
import { processVote, getRoutingKey, regionalCounts, userVotes } from '../../services/regional-rollup/index.js';

describe('regional-rollup - regional counting (real service logic)', () => {
  beforeEach(() => {
    for (const key in regionalCounts) delete regionalCounts[key];
    userVotes.clear();
  });

  describe('basic regional counting', () => {
    it('counts vote in correct region', () => {
      processVote('ana', 'Norte', 'A');
      expect(regionalCounts).toEqual({ Norte: { A: 1 } });
    });

    it('accumulates votes within same region', () => {
      processVote('ana', 'Norte', 'A');
      processVote('juan', 'Norte', 'A');
      processVote('pedro', 'Norte', 'A');
      expect(regionalCounts).toEqual({ Norte: { A: 3 } });
    });

    it('maintains separate counts per region', () => {
      processVote('ana', 'Norte', 'A');
      processVote('juan', 'Sur', 'A');
      processVote('pedro', 'Este', 'A');
      expect(regionalCounts).toEqual({
        Norte: { A: 1 },
        Sur: { A: 1 },
        Este: { A: 1 },
      });
    });

    it('handles multiple candidates per region', () => {
      processVote('ana', 'Norte', 'A');
      processVote('juan', 'Norte', 'B');
      processVote('pedro', 'Norte', 'C');
      expect(regionalCounts).toEqual({ Norte: { A: 1, B: 1, C: 1 } });
    });
  });

  describe('deduplication - user re-voting in same region', () => {
    it('replaces vote when user re-votes same candidate in same region', () => {
      processVote('ana', 'Norte', 'A');
      processVote('ana', 'Norte', 'A');
      expect(regionalCounts).toEqual({ Norte: { A: 1 } });
    });

    it('transfers vote when user changes candidate in same region', () => {
      processVote('ana', 'Norte', 'A');
      processVote('ana', 'Norte', 'B');
      expect(regionalCounts).toEqual({ Norte: { B: 1 } });
    });
  });

  describe('deduplication - user changing region', () => {
    it('removes vote from old region when user changes region', () => {
      processVote('ana', 'Norte', 'A');
      processVote('ana', 'Sur', 'A');
      expect(regionalCounts).toEqual({ Sur: { A: 1 } });
      expect(regionalCounts.Norte).toBeUndefined();
    });

    it('removes old region entirely when last voter leaves', () => {
      processVote('ana', 'Norte', 'A');
      processVote('ana', 'Sur', 'A');
      expect(Object.keys(regionalCounts)).toEqual(['Sur']);
    });

    it('keeps old region if other voters remain', () => {
      processVote('ana', 'Norte', 'A');
      processVote('juan', 'Norte', 'B');
      processVote('ana', 'Sur', 'A');
      expect(regionalCounts).toEqual({ Norte: { B: 1 }, Sur: { A: 1 } });
    });

    it('handles user switching region and candidate simultaneously', () => {
      processVote('ana', 'Norte', 'A');
      processVote('ana', 'Sur', 'B');
      expect(regionalCounts).toEqual({ Sur: { B: 1 } });
      expect(regionalCounts.Norte).toBeUndefined();
    });

    it('handles multiple region switches', () => {
      processVote('ana', 'Norte', 'A');
      processVote('ana', 'Sur', 'A');
      processVote('ana', 'Este', 'A');
      processVote('ana', 'Oeste', 'A');
      expect(regionalCounts).toEqual({ Oeste: { A: 1 } });
      expect(regionalCounts.Norte).toBeUndefined();
      expect(regionalCounts.Sur).toBeUndefined();
      expect(regionalCounts.Este).toBeUndefined();
    });
  });

  describe('edge cases', () => {
    it('handles rapid region changes by same user', () => {
      const regions = ['Norte', 'Sur', 'Este', 'Oeste'];
      for (let i = 0; i < 20; i++) {
        processVote('ana', regions[i % 4], 'A');
      }
      expect(regionalCounts).toEqual({ Oeste: { A: 1 } });
      expect(Object.keys(regionalCounts).length).toBe(1);
    });

    it('handles large number of voters across regions', () => {
      for (let i = 0; i < 500; i++) {
        const region = ['Norte', 'Sur', 'Este', 'Oeste'][i % 4];
        const candidate = ['A', 'B', 'C'][i % 3];
        processVote(`user${i}`, region, candidate);
      }
      const totalVotes = Object.values(regionalCounts).reduce(
        (sum, region) => sum + Object.values(region).reduce((a, b) => a + b, 0),
        0
      );
      expect(totalVotes).toBe(500);
    });

    it('removes empty regions from state', () => {
      processVote('ana', 'Norte', 'A');
      processVote('ana', 'Sur', 'A');
      expect(regionalCounts.Norte).toBeUndefined();
    });
  });

  describe('routing key generation (real exported function)', () => {
    it('generates correct routing key for simple region', () => {
      expect(getRoutingKey('Norte')).toBe('results.norte');
    });

    it('generates correct routing key for region with spaces', () => {
      expect(getRoutingKey('America del Norte')).toBe('results.america_del_norte');
    });

    it('generates correct routing key for region with hyphens', () => {
      expect(getRoutingKey('Norte-Sur')).toBe('results.norte-sur');
    });

    it('routing key matches topic exchange binding pattern', () => {
      const regions = ['Norte', 'Sur', 'Este', 'Oeste'];
      const pattern = /^results\.[a-z_\-]+$/;
      regions.forEach((region) => {
        expect(pattern.test(getRoutingKey(region))).toBe(true);
      });
    });
  });
});
