import { describe, it, expect, beforeEach } from 'vitest';
import { processVote, globalCounts, userVotes } from '../../services/vote-processor/index.js';

describe('vote-processor - global counting (real service logic)', () => {
  beforeEach(() => {
    for (const key in globalCounts) delete globalCounts[key];
    userVotes.clear();
  });

  describe('basic vote counting', () => {
    it('counts single vote for a candidate', () => {
      processVote('ana', 'A');
      expect(globalCounts).toEqual({ A: 1 });
    });

    it('accumulates votes for same candidate from different users', () => {
      processVote('ana', 'A');
      processVote('juan', 'A');
      processVote('pedro', 'A');
      expect(globalCounts).toEqual({ A: 3 });
    });

    it('counts votes for multiple candidates', () => {
      processVote('ana', 'A');
      processVote('juan', 'B');
      processVote('pedro', 'C');
      expect(globalCounts).toEqual({ A: 1, B: 1, C: 1 });
    });

    it('handles mixed candidate voting', () => {
      processVote('ana', 'A');
      processVote('juan', 'A');
      processVote('pedro', 'B');
      processVote('lucia', 'B');
      processVote('carlos', 'C');
      expect(globalCounts).toEqual({ A: 2, B: 2, C: 1 });
    });
  });

  describe('deduplication - one vote per user', () => {
    it('replaces previous vote when user re-votes same candidate', () => {
      processVote('ana', 'A');
      processVote('ana', 'A');
      expect(globalCounts).toEqual({ A: 1 });
    });

    it('transfers vote when user changes candidate', () => {
      processVote('ana', 'A');
      processVote('ana', 'B');
      expect(globalCounts).toEqual({ B: 1 });
    });

    it('handles multiple candidate switches by same user', () => {
      processVote('ana', 'A');
      processVote('ana', 'B');
      processVote('ana', 'C');
      expect(globalCounts).toEqual({ C: 1 });
    });

    it('does not affect other users when one user re-votes', () => {
      processVote('ana', 'A');
      processVote('juan', 'B');
      processVote('ana', 'C');
      expect(globalCounts).toEqual({ B: 1, C: 1 });
    });

    it('total votes always equals unique voting users', () => {
      const total = () => Object.values(globalCounts).reduce((a, b) => a + b, 0);
      processVote('ana', 'A');
      expect(total()).toBe(1);
      processVote('juan', 'B');
      expect(total()).toBe(2);
      processVote('ana', 'C');
      expect(total()).toBe(2);
      processVote('pedro', 'A');
      expect(total()).toBe(3);
      processVote('juan', 'A');
      expect(total()).toBe(3);
    });
  });

  describe('edge cases', () => {
    it('handles rapid re-voting by same user', () => {
      for (let i = 0; i < 100; i++) {
        processVote('ana', i % 2 === 0 ? 'A' : 'B');
      }
      expect(globalCounts).toEqual({ B: 1 });
    });

    it('handles large number of unique voters', () => {
      for (let i = 0; i < 1000; i++) {
        processVote(`user${i}`, i % 3 === 0 ? 'A' : i % 3 === 1 ? 'B' : 'C');
      }
      const total = Object.values(globalCounts).reduce((a, b) => a + b, 0);
      expect(total).toBe(1000);
    });

    it('removes candidate from counts when count reaches zero', () => {
      processVote('ana', 'A');
      processVote('juan', 'A');
      processVote('ana', 'B');
      processVote('juan', 'B');
      expect(globalCounts).toEqual({ B: 2 });
      expect(globalCounts.A).toBeUndefined();
    });
  });

  describe('state tracking', () => {
    it('tracks last vote per user in userVotes Map', () => {
      processVote('ana', 'A');
      processVote('ana', 'B');
      expect(userVotes.get('ana')).toBe('B');
    });

    it('userVotes size equals number of unique voters', () => {
      processVote('ana', 'A');
      processVote('juan', 'B');
      processVote('ana', 'C');
      expect(userVotes.size).toBe(2);
    });
  });

  describe('simulated Kafka compacted topic consumption', () => {
    it('handles pre-compaction duplicates correctly', () => {
      processVote('ana', 'A');
      processVote('ana', 'A');
      processVote('ana', 'A');
      expect(globalCounts).toEqual({ A: 1 });
    });

    it('handles interleaved votes from multiple users before compaction', () => {
      processVote('ana', 'A');
      processVote('juan', 'B');
      processVote('ana', 'C');
      processVote('juan', 'A');
      processVote('ana', 'A');
      processVote('juan', 'B');
      expect(globalCounts).toEqual({ A: 1, B: 1 });
    });
  });
});
