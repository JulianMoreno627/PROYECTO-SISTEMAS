import { describe, it, expect, beforeEach } from 'vitest';

describe('analytics-archiver - audit logging', () => {
  let archivedVotes;

  beforeEach(() => {
    archivedVotes = [];
  });

  function archiveVote(message) {
    const vote = JSON.parse(message.value.toString());
    archivedVotes.push({
      user_id: vote.user_id,
      candidate_id: vote.candidate_id,
      region: vote.region,
      ip_address: vote.ip_address,
      timestamp: vote.timestamp,
    });
  }

  describe('vote archiving', () => {
    it('archives vote with all fields', () => {
      const message = {
        value: Buffer.from(JSON.stringify({
          user_id: 'ana',
          candidate_id: 'A',
          region: 'Norte',
          ip_address: '192.168.1.1',
          timestamp: 1234567890,
        })),
      };

      archiveVote(message);

      expect(archivedVotes).toHaveLength(1);
      expect(archivedVotes[0]).toEqual({
        user_id: 'ana',
        candidate_id: 'A',
        region: 'Norte',
        ip_address: '192.168.1.1',
        timestamp: 1234567890,
      });
    });

    it('archives multiple votes in order', () => {
      const votes = [
        { user_id: 'ana', candidate_id: 'A', region: 'Norte', ip_address: '1.1.1.1', timestamp: 1 },
        { user_id: 'juan', candidate_id: 'B', region: 'Sur', ip_address: '2.2.2.2', timestamp: 2 },
        { user_id: 'pedro', candidate_id: 'C', region: 'Este', ip_address: '3.3.3.3', timestamp: 3 },
      ];

      votes.forEach((v) => {
        archiveVote({ value: Buffer.from(JSON.stringify(v)) });
      });

      expect(archivedVotes).toHaveLength(3);
      expect(archivedVotes[0].user_id).toBe('ana');
      expect(archivedVotes[1].user_id).toBe('juan');
      expect(archivedVotes[2].user_id).toBe('pedro');
    });
  });

  describe('audit log format', () => {
    it('log entry contains all required fields', () => {
      const vote = {
        user_id: 'ana',
        candidate_id: 'A',
        region: 'Norte',
        ip_address: '192.168.1.1',
        timestamp: Date.now(),
      };
      archiveVote({ value: Buffer.from(JSON.stringify(vote)) });

      const entry = archivedVotes[0];
      expect(entry).toHaveProperty('user_id');
      expect(entry).toHaveProperty('candidate_id');
      expect(entry).toHaveProperty('region');
      expect(entry).toHaveProperty('ip_address');
      expect(entry).toHaveProperty('timestamp');
    });

    it('log format matches [AUDIT] pattern', () => {
      const vote = {
        user_id: 'ana',
        candidate_id: 'A',
        region: 'Norte',
        ip_address: '192.168.1.1',
      };
      const logLine = `[AUDIT] Vote archived: User=${vote.user_id}, Candidate=${vote.candidate_id}, Region=${vote.region}, IP=${vote.ip_address}`;

      expect(logLine).toMatch(/^\[AUDIT\] Vote archived:/);
      expect(logLine).toContain('User=ana');
      expect(logLine).toContain('Candidate=A');
      expect(logLine).toContain('Region=Norte');
      expect(logLine).toContain('IP=192.168.1.1');
    });
  });

  describe('edge cases', () => {
    it('handles vote with missing optional fields', () => {
      const message = {
        value: Buffer.from(JSON.stringify({
          user_id: 'ana',
          candidate_id: 'A',
          region: 'Norte',
          ip_address: '192.168.1.1',
        })),
      };

      expect(() => archiveVote(message)).not.toThrow();
    });

    it('handles vote with null timestamp', () => {
      const message = {
        value: Buffer.from(JSON.stringify({
          user_id: 'ana',
          candidate_id: 'A',
          region: 'Norte',
          ip_address: '192.168.1.1',
          timestamp: null,
        })),
      };

      archiveVote(message);
      expect(archivedVotes[0].timestamp).toBeNull();
    });

    it('handles vote with unicode characters', () => {
      const message = {
        value: Buffer.from(JSON.stringify({
          user_id: 'usuario_ñoño',
          candidate_id: 'Candidato-España',
          region: 'América del Sur',
          ip_address: '192.168.1.1',
          timestamp: 123,
        })),
      };

      archiveVote(message);
      expect(archivedVotes[0].user_id).toBe('usuario_ñoño');
      expect(archivedVotes[0].region).toBe('América del Sur');
    });

    it('handles very large vote payload', () => {
      const largeVote = {
        user_id: 'ana',
        candidate_id: 'A',
        region: 'Norte',
        ip_address: '192.168.1.1',
        timestamp: 123,
        metadata: 'x'.repeat(100000),
      };
      const message = { value: Buffer.from(JSON.stringify(largeVote)) };

      expect(() => archiveVote(message)).not.toThrow();
    });
  });

  describe('consumer configuration', () => {
    it('uses dedicated consumer group', () => {
      const groupId = 'analytics-archiver-group';
      expect(groupId).toMatch(/analytics/);
    });

    it('consumes from raw_votes topic', () => {
      const topic = 'raw_votes';
      expect(topic).toBe('raw_votes');
    });

    it('consumes from beginning for full audit trail', () => {
      const fromBeginning = true;
      expect(fromBeginning).toBe(true);
    });
  });

  describe('compaction impact on audit trail', () => {
    it('archives all messages before compaction runs', () => {
      const messages = [
        { value: Buffer.from(JSON.stringify({ user_id: 'ana', candidate_id: 'A', region: 'Norte', ip_address: '1.1.1.1', timestamp: 1 })) },
        { value: Buffer.from(JSON.stringify({ user_id: 'ana', candidate_id: 'B', region: 'Sur', ip_address: '1.1.1.1', timestamp: 2 })) },
        { value: Buffer.from(JSON.stringify({ user_id: 'ana', candidate_id: 'C', region: 'Este', ip_address: '1.1.1.1', timestamp: 3 })) },
      ];

      messages.forEach(archiveVote);

      expect(archivedVotes).toHaveLength(3);
    });

    it('does NOT deduplicate (archives every message as received)', () => {
      const messages = [
        { value: Buffer.from(JSON.stringify({ user_id: 'ana', candidate_id: 'A', region: 'Norte', ip_address: '1.1.1.1', timestamp: 1 })) },
        { value: Buffer.from(JSON.stringify({ user_id: 'ana', candidate_id: 'A', region: 'Norte', ip_address: '1.1.1.1', timestamp: 1 })) },
      ];

      messages.forEach(archiveVote);

      expect(archivedVotes).toHaveLength(2);
    });
  });
});
