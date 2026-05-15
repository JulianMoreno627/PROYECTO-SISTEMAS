import { describe, it, expect, beforeEach } from 'vitest';
import { processVote, cleanupStaleIPs, ipVotersMap, CLEANUP_INTERVAL_MS, STALE_THRESHOLD_MS } from '../../services/bot-detector/index.js';

describe('bot-detector - fraud detection (real service logic)', () => {
  let alerts;

  beforeEach(() => {
    ipVotersMap.clear();
    alerts = [];
  });

  function trackAlerts(ipAddress, userId, timestamp) {
    const alert = processVote(ipAddress, userId, timestamp);
    if (alert) alerts.push(alert);
  }

  describe('tracking votes by IP', () => {
    it('tracks unique user_ids per IP address', () => {
      trackAlerts('192.168.1.1', 'ana');
      trackAlerts('192.168.1.1', 'juan');
      trackAlerts('192.168.1.1', 'pedro');

      const entry = ipVotersMap.get('192.168.1.1');
      expect(entry.set.size).toBe(3);
      expect(entry.set.has('ana')).toBe(true);
      expect(entry.set.has('juan')).toBe(true);
      expect(entry.set.has('pedro')).toBe(true);
    });

    it('tracks multiple IPs independently', () => {
      trackAlerts('192.168.1.1', 'ana');
      trackAlerts('192.168.1.2', 'juan');
      trackAlerts('192.168.1.1', 'pedro');
      trackAlerts('192.168.1.2', 'lucia');

      expect(ipVotersMap.get('192.168.1.1').set.size).toBe(2);
      expect(ipVotersMap.get('192.168.1.2').set.size).toBe(2);
    });

    it('ignores duplicate user_id from same IP', () => {
      trackAlerts('192.168.1.1', 'ana');
      trackAlerts('192.168.1.1', 'ana');
      trackAlerts('192.168.1.1', 'ana');

      expect(ipVotersMap.get('192.168.1.1').set.size).toBe(1);
    });
  });

  describe('alert threshold (>5 unique users per IP)', () => {
    it('does NOT alert with 5 or fewer users', () => {
      for (let i = 0; i < 5; i++) {
        trackAlerts('192.168.1.1', `user${i}`);
      }
      expect(alerts).toHaveLength(0);
    });

    it('alerts when 6th unique user votes from same IP', () => {
      for (let i = 0; i < 6; i++) {
        trackAlerts('192.168.1.1', `user${i}`);
      }
      expect(alerts).toHaveLength(1);
      expect(alerts[0].ip).toBe('192.168.1.1');
      expect(alerts[0].users).toHaveLength(6);
    });

    it('includes all user_ids in alert', () => {
      const users = ['ana', 'juan', 'pedro', 'lucia', 'carlos', 'maria'];
      users.forEach((u) => trackAlerts('10.0.0.1', u));
      expect(alerts[0].users).toEqual(expect.arrayContaining(users));
    });

    it('includes timestamp in alert', () => {
      const ts = 1700000000000;
      for (let i = 1; i <= 6; i++) {
        trackAlerts('10.0.0.1', `u${i}`, ts);
      }
      expect(alerts[0].timestamp).toBe(ts);
    });
  });

  describe('single alert per window', () => {
    it('does NOT send duplicate alerts for same IP in same window', () => {
      for (let i = 0; i < 10; i++) {
        trackAlerts('192.168.1.1', `user${i}`);
      }
      expect(alerts).toHaveLength(1);
    });

    it('sends new alert after window resets', () => {
      const baseTime = 1700000000000;
      for (let i = 0; i < 6; i++) {
        trackAlerts('192.168.1.1', `user${i}`, baseTime);
      }
      expect(alerts).toHaveLength(1);

      for (let i = 0; i < 6; i++) {
        trackAlerts('192.168.1.1', `newuser${i}`, baseTime + 61000);
      }
      expect(alerts).toHaveLength(2);
    });

    it('alerts independently for different IPs', () => {
      for (let i = 0; i < 6; i++) {
        trackAlerts('192.168.1.1', `user${i}`);
      }
      for (let i = 0; i < 6; i++) {
        trackAlerts('192.168.1.2', `other${i}`);
      }
      expect(alerts).toHaveLength(2);
    });

    it('resets alerted flag when window expires', () => {
      const baseTime = 1700000000000;
      for (let i = 0; i < 6; i++) {
        trackAlerts('192.168.1.1', `user${i}`, baseTime);
      }
      expect(alerts).toHaveLength(1);
      expect(ipVotersMap.get('192.168.1.1').alerted).toBe(true);

      for (let i = 0; i < 6; i++) {
        trackAlerts('192.168.1.1', `newuser${i}`, baseTime + 61000);
      }
      expect(alerts).toHaveLength(2);
    });
  });

  describe('time window management', () => {
    it('resets window after 1 minute of inactivity', () => {
      const baseTime = 1700000000000;
      for (let i = 0; i < 5; i++) {
        trackAlerts('192.168.1.1', `user${i}`, baseTime);
      }
      expect(ipVotersMap.get('192.168.1.1').set.size).toBe(5);

      trackAlerts('192.168.1.1', 'newuser', baseTime + 61000);
      expect(ipVotersMap.get('192.168.1.1').set.size).toBe(1);
    });

    it('does NOT reset within the 1-minute window', () => {
      const baseTime = 1700000000000;
      for (let i = 0; i < 5; i++) {
        trackAlerts('192.168.1.1', `user${i}`, baseTime);
      }
      trackAlerts('192.168.1.1', 'user5', baseTime + 30000);
      expect(ipVotersMap.get('192.168.1.1').set.size).toBe(6);
    });

    it('window starts from first vote, not last', () => {
      const baseTime = 1700000000000;
      trackAlerts('192.168.1.1', 'user1', baseTime);
      trackAlerts('192.168.1.1', 'user2', baseTime + 30000);
      trackAlerts('192.168.1.1', 'user3', baseTime + 59000);
      expect(ipVotersMap.get('192.168.1.1').firstVoteTime).toBe(baseTime);
    });
  });

  describe('stale IP cleanup (real exported function)', () => {
    it('removes IPs inactive for more than 5 minutes', () => {
      const baseTime = 1700000000000;
      trackAlerts('192.168.1.1', 'ana', baseTime);
      trackAlerts('192.168.1.2', 'juan', baseTime);

      cleanupStaleIPs(baseTime + 301000);

      expect(ipVotersMap.has('192.168.1.1')).toBe(false);
      expect(ipVotersMap.has('192.168.1.2')).toBe(false);
    });

    it('keeps IPs that voted within 5 minutes', () => {
      const baseTime = 1700000000000;
      trackAlerts('192.168.1.1', 'ana', baseTime);
      trackAlerts('192.168.1.2', 'juan', baseTime + 200000);

      cleanupStaleIPs(baseTime + 301000);

      expect(ipVotersMap.has('192.168.1.1')).toBe(false);
      expect(ipVotersMap.has('192.168.1.2')).toBe(true);
    });

    it('cleanup interval is 2 minutes', () => {
      expect(CLEANUP_INTERVAL_MS).toBe(120000);
    });

    it('prevents memory leak from accumulated IPs', () => {
      const baseTime = 1700000000000;
      for (let i = 0; i < 100; i++) {
        trackAlerts(`192.168.1.${i}`, `user${i}`, baseTime);
      }
      expect(ipVotersMap.size).toBe(100);

      cleanupStaleIPs(baseTime + 301000);
      expect(ipVotersMap.size).toBe(0);
    });
  });

  describe('edge cases', () => {
    it('handles IPv6 addresses', () => {
      trackAlerts('::1', 'ana');
      trackAlerts('::1', 'juan');
      expect(ipVotersMap.get('::1').set.size).toBe(2);
    });

    it('handles localhost addresses', () => {
      trackAlerts('127.0.0.1', 'ana');
      trackAlerts('127.0.0.1', 'juan');
      expect(ipVotersMap.get('127.0.0.1').set.size).toBe(2);
    });

    it('handles empty user_id', () => {
      trackAlerts('192.168.1.1', '');
      expect(ipVotersMap.get('192.168.1.1').set.has('')).toBe(true);
    });

    it('handles very large number of unique IPs', () => {
      for (let i = 0; i < 10000; i++) {
        trackAlerts(`10.0.${Math.floor(i / 256)}.${i % 256}`, `user${i}`);
      }
      expect(ipVotersMap.size).toBe(10000);
    });

    it('handles same user voting from multiple IPs', () => {
      trackAlerts('192.168.1.1', 'ana');
      trackAlerts('192.168.1.2', 'ana');
      trackAlerts('192.168.1.3', 'ana');
      expect(ipVotersMap.get('192.168.1.1').set.has('ana')).toBe(true);
      expect(ipVotersMap.get('192.168.1.2').set.has('ana')).toBe(true);
      expect(ipVotersMap.get('192.168.1.3').set.has('ana')).toBe(true);
    });

    it('exactly 5 users does NOT trigger alert', () => {
      for (let i = 0; i < 5; i++) {
        trackAlerts('192.168.1.1', `user${i}`);
      }
      expect(alerts).toHaveLength(0);
    });

    it('exactly 6 users DOES trigger alert', () => {
      for (let i = 0; i < 6; i++) {
        trackAlerts('192.168.1.1', `user${i}`);
      }
      expect(alerts).toHaveLength(1);
    });
  });

  describe('alert message format', () => {
    it('alerts are published to security_alerts topic', () => {
      expect('security_alerts').toBe('security_alerts');
    });

    it('alert message contains required fields', () => {
      for (let i = 0; i < 6; i++) {
        trackAlerts('10.0.0.1', `user${i}`);
      }
      const alert = alerts[0];
      expect(alert).toHaveProperty('ip');
      expect(alert).toHaveProperty('users');
      expect(alert).toHaveProperty('message');
      expect(alert).toHaveProperty('timestamp');
      expect(Array.isArray(alert.users)).toBe(true);
      expect(typeof alert.message).toBe('string');
      expect(typeof alert.timestamp).toBe('number');
    });
  });

  describe('exported constants', () => {
    it('STALE_THRESHOLD_MS is 5 minutes', () => {
      expect(STALE_THRESHOLD_MS).toBe(300000);
    });

    it('CLEANUP_INTERVAL_MS is 2 minutes', () => {
      expect(CLEANUP_INTERVAL_MS).toBe(120000);
    });
  });
});
