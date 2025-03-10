const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const cron = require('node-cron');
const ErrsolePostgres = require('../lib/index'); // Adjust the path as needed
const { describe } = require('@jest/globals');
/* globals expect, jest, beforeEach, it, afterEach, describe, afterAll */

jest.mock('pg', () => {
  const mClient = {
    query: jest.fn(),
    release: jest.fn()
  };
  const mPool = {
    connect: jest.fn().mockResolvedValue(mClient),
    query: jest.fn().mockResolvedValue({ rows: [{ work_mem: '8192kB' }] })
  };
  return { Pool: jest.fn(() => mPool) };
});

jest.mock('bcryptjs', () => ({
  hash: jest.fn(),
  compare: jest.fn()
}));

describe('ErrsolePostgres', () => {
  let errsolePostgres;
  let poolMock;
  let clientMock;
  let originalConsoleError;
  let cronJob;

  beforeEach(() => {
    clientMock = {
      query: jest.fn().mockResolvedValue({ rows: [{ work_mem: '8192kB' }] }),
      release: jest.fn()
    };

    poolMock = {
      connect: jest.fn().mockResolvedValue(clientMock),
      query: jest.fn().mockImplementation((query, values) => {
        if (query.includes('SHOW work_mem')) {
          return Promise.resolve({ rows: [{ work_mem: '8192kB' }] }); // Mock for getWorkMem
        }
        if (query.includes('INSERT INTO')) {
          return Promise.resolve({ rows: [{ id: 1 }] }); // Mock for createUser
        }
        return Promise.resolve({ rows: [] });
      })
    };

    Pool.mockImplementation(() => poolMock);

    errsolePostgres = new ErrsolePostgres({
      host: 'localhost',
      user: 'root',
      password: 'password',
      database: 'dbname'
    });

    // Mock setInterval and cron.schedule
    jest.useFakeTimers();
    jest.spyOn(global, 'setInterval');
    cronJob = { stop: jest.fn() };
    jest.spyOn(cron, 'schedule').mockReturnValue(cronJob);

    // Suppress console.error
    originalConsoleError = console.error;
    console.error = jest.fn();
  });

  afterEach(() => {
    jest.clearAllMocks();
    jest.useRealTimers();
    // Restore console.error
    console.error = originalConsoleError;
  });

  describe('#initialize', () => {
    it('should initialize properly', async () => {
      await errsolePostgres.initialize();

      expect(poolMock.connect).toHaveBeenCalled();
      expect(poolMock.query).toHaveBeenCalledWith(expect.any(String));
      expect(errsolePostgres.isConnectionInProgress).toBe(false);
      // Check if setInterval and cron.schedule were called
      expect(setInterval).toHaveBeenCalled();
      expect(cron.schedule).toHaveBeenCalled();
    });
  });

  describe('#getWorkMem', () => {
    let poolQuerySpy;

    beforeEach(() => {
      poolQuerySpy = jest.spyOn(poolMock, 'query');
      poolMock.query.mockClear(); // Clear any previous calls
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should return the current work_mem value', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [{ work_mem: '8192kB' }] });
      const result = await errsolePostgres.getWorkMem();

      expect(poolQuerySpy).toHaveBeenCalledWith('SHOW work_mem');
      expect(result).toBe(8192);
    });

    it('should handle errors during the query execution', async () => {
      const error = new Error('Query error');
      poolQuerySpy.mockRejectedValueOnce(error);

      await expect(errsolePostgres.getWorkMem()).rejects.toThrow('Query error');
      expect(poolQuerySpy).toHaveBeenCalledWith('SHOW work_mem');
    });

    it('should return NaN if work_mem value is not a number', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [{ work_mem: 'not_a_number' }] });
      const result = await errsolePostgres.getWorkMem();

      expect(poolQuerySpy).toHaveBeenCalledWith('SHOW work_mem');
      expect(result).toBeNaN();
    });
  });

  describe('#checkConnection', () => {
    it('should successfully check the database connection', async () => {
      await expect(errsolePostgres.checkConnection()).resolves.not.toThrow();

      expect(poolMock.connect).toHaveBeenCalled();
      expect(clientMock.query).toHaveBeenCalledWith('SELECT NOW()');
      expect(clientMock.release).toHaveBeenCalled();
    });

    it('should throw an error if query fails', async () => {
      clientMock.query.mockRejectedValueOnce(new Error('Query error'));

      await expect(errsolePostgres.checkConnection()).rejects.toThrow('Query error');

      expect(poolMock.connect).toHaveBeenCalled();
      expect(clientMock.query).toHaveBeenCalledWith('SELECT NOW()');
      expect(clientMock.release).toHaveBeenCalled();
    });

    it('should release the client even if the query fails', async () => {
      clientMock.query.mockRejectedValueOnce(new Error('Query error'));

      try {
        await errsolePostgres.checkConnection();
      } catch (error) {
        expect(error.message).toBe('Query error');
      }

      expect(clientMock.release).toHaveBeenCalled();
    });
  });

  describe('#setWorkMem', () => {
    let poolQuerySpy;
    let getWorkMemSpy;

    beforeEach(() => {
      poolQuerySpy = jest.spyOn(poolMock, 'query');
      getWorkMemSpy = jest.spyOn(errsolePostgres, 'getWorkMem');
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should set work_mem if current size is less than desired', async () => {
      getWorkMemSpy.mockResolvedValueOnce(4096); // Current size less than desired

      await expect(errsolePostgres.setWorkMem()).resolves.not.toThrow();

      expect(getWorkMemSpy).toHaveBeenCalled();
      expect(poolQuerySpy).toHaveBeenCalledWith("SET work_mem = '8192kB'");
    });

    it('should handle errors during setting work_mem', async () => {
      const error = new Error('Query error');
      getWorkMemSpy.mockResolvedValueOnce(4096); // Current size less than desired
      poolQuerySpy.mockRejectedValueOnce(error);

      await expect(errsolePostgres.setWorkMem()).rejects.toThrow('Query error');

      expect(getWorkMemSpy).toHaveBeenCalled();
      expect(poolQuerySpy).toHaveBeenCalledWith("SET work_mem = '8192kB'");
    });
  });

  describe('#createTables', () => {
    it('should create necessary tables', async () => {
      // Mock the query method to resolve for all calls
      poolMock.query.mockResolvedValue({});

      // Call the function
      await errsolePostgres.createTables();

      // Capture all queries executed
      const executedQueries = poolMock.query.mock.calls.map(call => call[0]);
      // Define expected table creation queries
      const expectedQueries = [
        /CREATE TABLE IF NOT EXISTS errsole_logs_v3/,
        /CREATE INDEX IF NOT EXISTS .*errsole_logs_v3.*hostname.*source.*level.*timestamp.*id/,
        /CREATE INDEX IF NOT EXISTS .*errsole_logs_v3.*hostname.*timestamp.*id/,
        /CREATE INDEX IF NOT EXISTS .*errsole_logs_v3.*hostname/,
        /CREATE INDEX IF NOT EXISTS .*errsole_logs_v3.*timestamp.*id/,
        /CREATE INDEX IF NOT EXISTS .*errsole_logs_v3.*errsole_id/,
        /CREATE TABLE IF NOT EXISTS errsole_users/,
        /CREATE TABLE IF NOT EXISTS errsole_config/
      ];

      // Ensure all expected queries were executed
      expectedQueries.forEach(expectedQuery => {
        expect(executedQueries.some(query => expectedQuery.test(query))).toBe(true);
      });
    });

    it('should throw an error if table creation fails', async () => {
      const error = new Error('Query error');
      poolMock.query.mockRejectedValueOnce(error);

      await expect(errsolePostgres.createTables()).rejects.toThrow('Query error');

      expect(poolMock.query).toHaveBeenCalled();
      expect(errsolePostgres.isConnectionInProgress).toBe(false);
    });
  });

  describe('#getConfig', () => {
    it('should retrieve a configuration based on the provided key', async () => {
      const config = { key: 'testKey', value: 'testValue' };
      poolMock.query.mockResolvedValueOnce({ rows: [config] });

      const result = await errsolePostgres.getConfig('testKey');

      expect(poolMock.query).toHaveBeenCalledWith('SELECT * FROM errsole_config WHERE key = $1', ['testKey']);
      expect(result).toEqual({ item: config });
    });

    it('should return null if configuration key is not found', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [] });

      const result = await errsolePostgres.getConfig('nonexistentKey');

      expect(poolMock.query).toHaveBeenCalledWith('SELECT * FROM errsole_config WHERE key = $1', ['nonexistentKey']);
      expect(result).toEqual({ item: null });
    });

    it('should handle errors during the query execution', async () => {
      poolMock.query.mockRejectedValueOnce(new Error('Query error'));

      await expect(errsolePostgres.getConfig('testKey')).rejects.toThrow('Query error');
      expect(poolMock.query).toHaveBeenCalledWith('SELECT * FROM errsole_config WHERE key = $1', ['testKey']);
    });
  });

  describe('#deleteConfig', () => {
    beforeEach(() => {
      poolMock.query.mockClear(); // Reset the mock for each test
    });

    it('should delete config by key', async () => {
      poolMock.query.mockResolvedValueOnce({ rowCount: 1 });

      const result = await errsolePostgres.deleteConfig('logsTTL');

      expect(poolMock.query).toHaveBeenCalledWith('DELETE FROM errsole_config WHERE key = $1', ['logsTTL']);
      expect(result).toEqual({});
    });

    it('should handle errors during the deleteConfig operation', async () => {
      poolMock.query.mockRejectedValueOnce(new Error('Query error'));

      await expect(errsolePostgres.deleteConfig('logsTTL')).rejects.toThrow('Query error');
      expect(poolMock.query).toHaveBeenCalledWith('DELETE FROM errsole_config WHERE key = $1', ['logsTTL']);
    });
  });

  describe('#postLogs', () => {
    it('should add log entries to pending logs', () => {
      const logEntries = [
        { timestamp: new Date(), hostname: 'localhost', pid: 1234, source: 'test', level: 'info', message: 'test message', meta: 'meta' }
      ];
      errsolePostgres.postLogs(logEntries);

      expect(errsolePostgres.pendingLogs).toHaveLength(1);
      expect(errsolePostgres.pendingLogs[0]).toEqual(logEntries[0]);
    });

    it('should call flushLogs if pending logs exceed batch size', async () => {
      const logEntries = Array.from({ length: errsolePostgres.batchSize + 1 }, (_, i) => ({
        timestamp: new Date(),
        hostname: 'localhost',
        pid: 1234,
        source: 'test',
        level: 'info',
        message: `test message ${i}`,
        meta: 'meta'
      }));

      const flushLogsSpy = jest.spyOn(errsolePostgres, 'flushLogs').mockImplementation(() => Promise.resolve({}));

      errsolePostgres.postLogs(logEntries);

      expect(flushLogsSpy).toHaveBeenCalled();
    });
  });

  describe('#verifyUser', () => {
    it('should throw an error if email is missing', async () => {
      await expect(errsolePostgres.verifyUser(null, 'password'))
        .rejects.toThrow('Both email and password are required for verification.');
    });

    it('should throw an error if password is missing', async () => {
      await expect(errsolePostgres.verifyUser('email@example.com', null))
        .rejects.toThrow('Both email and password are required for verification.');
    });

    it('should throw an error if user is not found', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [] });

      await expect(errsolePostgres.verifyUser('email@example.com', 'password'))
        .rejects.toThrow('User not found.');
    });

    it('should throw an error if the password is incorrect', async () => {
      const user = { email: 'email@example.com', hashed_password: 'hashed_password' };
      poolMock.query.mockResolvedValueOnce({ rows: [user] });
      bcrypt.compare.mockResolvedValueOnce(false);

      await expect(errsolePostgres.verifyUser('email@example.com', 'wrongpassword'))
        .rejects.toThrow('Incorrect password.');
    });

    it('should return the user object if email and password are correct', async () => {
      const user = { id: 1, email: 'email@example.com', name: 'John Doe', hashed_password: 'hashed_password' };
      poolMock.query.mockResolvedValueOnce({ rows: [user] });
      bcrypt.compare.mockResolvedValueOnce(true);

      const result = await errsolePostgres.verifyUser('email@example.com', 'password');

      expect(result).toEqual({ item: { id: 1, email: 'email@example.com', name: 'John Doe' } });
    });
  });

  describe('#getAllUsers', () => {
    it('should successfully retrieve all users', async () => {
      const users = [
        { id: 1, name: 'John Doe', email: 'john@example.com', role: 'admin' },
        { id: 2, name: 'Jane Smith', email: 'jane@example.com', role: 'user' }
      ];
      poolMock.query.mockResolvedValueOnce({ rows: users });

      const result = await errsolePostgres.getAllUsers();

      expect(poolMock.query).toHaveBeenCalledWith('SELECT id, name, email, role FROM errsole_users');
      expect(result).toEqual({ items: users });
    });

    it('should return an empty array if no users are found', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [] });

      const result = await errsolePostgres.getAllUsers();

      expect(poolMock.query).toHaveBeenCalledWith('SELECT id, name, email, role FROM errsole_users');
      expect(result).toEqual({ items: [] });
    });

    it('should handle errors during query execution', async () => {
      const error = new Error('Query error');
      poolMock.query.mockRejectedValueOnce(error);

      await expect(errsolePostgres.getAllUsers()).rejects.toThrow('Query error');

      expect(poolMock.query).toHaveBeenCalledWith('SELECT id, name, email, role FROM errsole_users');
    });
  });

  describe('#updateUserByEmail', () => {
    let poolQuerySpy;
    let getUserByEmailSpy;

    beforeEach(() => {
      poolQuerySpy = jest.spyOn(poolMock, 'query');
      getUserByEmailSpy = jest.spyOn(errsolePostgres, 'getUserByEmail').mockResolvedValue({ item: { id: 1, name: 'updated', email: 'test@example.com', role: 'admin' } });
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should update user by email', async () => {
      poolQuerySpy.mockResolvedValue({ rowCount: 1 });

      const user = await errsolePostgres.updateUserByEmail('test@example.com', { name: 'updated' });

      expect(poolQuerySpy).toHaveBeenCalledWith(
        'UPDATE errsole_users SET name = $1 WHERE email = $2',
        ['updated', 'test@example.com']
      );
      expect(getUserByEmailSpy).toHaveBeenCalledWith('test@example.com');
      expect(user).toEqual({ item: { id: 1, name: 'updated', email: 'test@example.com', role: 'admin' } });
    });

    it('should throw an error if no email is provided', async () => {
      await expect(errsolePostgres.updateUserByEmail('', { name: 'updated' })).rejects.toThrow('Email is required.');
    });

    it('should throw an error if no updates are provided', async () => {
      await expect(errsolePostgres.updateUserByEmail('test@example.com', {})).rejects.toThrow('No updates provided.');
    });

    it('should throw an error if no updates are applied', async () => {
      poolQuerySpy.mockResolvedValue({ rowCount: 0 });

      await expect(errsolePostgres.updateUserByEmail('test@example.com', { name: 'updated' })).rejects.toThrow('No updates applied.');
    });

    it('should handle restricted fields', async () => {
      poolQuerySpy.mockResolvedValue({ rowCount: 1 });

      await errsolePostgres.updateUserByEmail('test@example.com', { name: 'updated', id: 2, hashed_password: 'secret' });

      expect(poolQuerySpy).toHaveBeenCalledWith(
        'UPDATE errsole_users SET name = $1 WHERE email = $2',
        ['updated', 'test@example.com']
      );
    });

    it('should handle query errors during user update', async () => {
      poolQuerySpy.mockRejectedValue(new Error('Query error'));

      await expect(errsolePostgres.updateUserByEmail('test@example.com', { name: 'updated' })).rejects.toThrow('Query error');
    });
  });

  describe('#updatePassword', () => {
    it('should update user password', async () => {
      const user = { id: 1, name: 'test', email: 'test@example.com', hashed_password: 'hashedPassword', role: 'admin' };
      poolMock.query
        .mockResolvedValueOnce({ rows: [user] }) // First query response
        .mockResolvedValueOnce({ rowCount: 1 }); // Second query response
      bcrypt.compare.mockResolvedValue(true);
      bcrypt.hash.mockResolvedValue('newHashedPassword');

      const result = await errsolePostgres.updatePassword('test@example.com', 'password', 'newPassword');

      expect(poolMock.query).toHaveBeenCalledWith('SELECT * FROM errsole_users WHERE email = $1', ['test@example.com']);
      expect(bcrypt.compare).toHaveBeenCalledWith('password', 'hashedPassword');
      expect(bcrypt.hash).toHaveBeenCalledWith('newPassword', 10);
      expect(poolMock.query).toHaveBeenCalledWith('UPDATE errsole_users SET hashed_password = $1 WHERE email = $2', ['newHashedPassword', 'test@example.com']);
      expect(result).toEqual({ item: { id: 1, name: 'test', email: 'test@example.com', role: 'admin' } });
    });

    it('should throw an error if email, current password, or new password is missing', async () => {
      await expect(errsolePostgres.updatePassword('', 'password', 'newPassword')).rejects.toThrow('Email, current password, and new password are required.');
      await expect(errsolePostgres.updatePassword('test@example.com', '', 'newPassword')).rejects.toThrow('Email, current password, and new password are required.');
      await expect(errsolePostgres.updatePassword('test@example.com', 'password', '')).rejects.toThrow('Email, current password, and new password are required.');
    });

    it('should throw an error if user is not found', async () => {
      poolMock.query.mockResolvedValue({ rows: [] });

      await expect(errsolePostgres.updatePassword('test@example.com', 'password', 'newPassword')).rejects.toThrow('User not found.');
    });

    it('should throw an error if current password is incorrect', async () => {
      const user = { id: 1, name: 'test', email: 'test@example.com', hashed_password: 'hashedPassword', role: 'admin' };
      poolMock.query.mockResolvedValue({ rows: [user] });
      bcrypt.compare.mockResolvedValue(false);

      await expect(errsolePostgres.updatePassword('test@example.com', 'wrongPassword', 'newPassword')).rejects.toThrow('Current password is incorrect.');
    });
  });

  describe('#getUserByEmail', () => {
    let poolQuerySpy;

    beforeEach(() => {
      poolQuerySpy = jest.spyOn(poolMock, 'query');
    });

    afterEach(() => {
      jest.clearAllMocks();
    });
    it('should throw an error if no email is provided', async () => {
      poolQuerySpy.mockClear();

      await expect(errsolePostgres.getUserByEmail()).rejects.toThrow('Email is required.');

      expect(poolQuerySpy).not.toHaveBeenCalled(); // Ensures no query is made for this case
    });

    it('should throw an error if the user is not found', async () => {
      poolQuerySpy.mockResolvedValueOnce({ rows: [] });

      await expect(errsolePostgres.getUserByEmail('nonexistent@example.com')).rejects.toThrow('User not found.');

      expect(poolQuerySpy).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, name, email, role FROM'),
        ['nonexistent@example.com']
      );
    });

    it('should return the user object if the user is found', async () => {
      const user = { id: 1, name: 'John Doe', email: 'john@example.com', role: 'admin' };
      poolQuerySpy.mockResolvedValueOnce({ rows: [user] });

      const result = await errsolePostgres.getUserByEmail('john@example.com');

      expect(poolQuerySpy).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, name, email, role FROM'),
        ['john@example.com']
      );
      expect(result).toEqual({ item: user });
    });

    it('should handle database errors gracefully', async () => {
      const error = new Error('Database query failed');
      poolQuerySpy.mockRejectedValueOnce(error);

      await expect(errsolePostgres.getUserByEmail('john@example.com')).rejects.toThrow('Database query failed');

      expect(poolQuerySpy).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, name, email, role FROM'),
        ['john@example.com']
      );
    });
  });

  describe('#getLogs', () => {
    let poolQuerySpy;

    beforeEach(() => {
      poolQuerySpy = jest.spyOn(poolMock, 'query');
    });
    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should retrieve logs with no filters', async () => {
      const logs = [
        { id: 1, hostname: 'localhost', pid: 1234, source: 'test', timestamp: new Date(), level: 'info', message: 'test message' }
      ];
      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.getLogs();

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3'),
        [100]
      );
      expect(result).toEqual({ items: logs });
    });

    it('should apply lt_id filter', async () => {
      const logs = [
        { id: 1, hostname: 'localhost', pid: 1234, source: 'test', timestamp: new Date(), level: 'info', message: 'test message' }
      ];
      const filters = {
        lt_id: 10,
        limit: 50
      };
      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3 WHERE id < $1 ORDER BY id DESC LIMIT $2'),
        [10, 50]
      );
      expect(result).toEqual({ items: logs });
    });

    it('should apply gt_id filter', async () => {
      const logs = [
        { id: 1, hostname: 'localhost', pid: 1234, source: 'test', timestamp: new Date(), level: 'info', message: 'test message' }
      ];
      const filters = {
        gt_id: 5,
        limit: 50
      };
      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3 WHERE id > $1 ORDER BY id ASC LIMIT $2'),
        [5, 50]
      );
      expect(result).toEqual({ items: logs });
    });

    it('should apply lte_timestamp filter', async () => {
      const logs = [
        { id: 1, hostname: 'localhost', pid: 1234, source: 'test', timestamp: new Date('2023-01-01T00:00:00Z'), level: 'info', message: 'test message' }
      ];
      const filters = {
        lte_timestamp: new Date('2023-01-02T00:00:00Z'),
        limit: 50
      };

      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3 WHERE timestamp <= $1 ORDER BY timestamp DESC, id DESC LIMIT $2'),
        [new Date('2023-01-02T00:00:00Z'), 50]
      );
      expect(result).toEqual({ items: logs });
    });

    it('should apply gte_timestamp filter', async () => {
      const logs = [
        { id: 1, hostname: 'localhost', pid: 1234, source: 'test', timestamp: new Date('2023-01-01T00:00:00Z'), level: 'info', message: 'test message' }
      ];
      const filters = {
        gte_timestamp: new Date('2023-01-01T00:00:00Z'),
        limit: 50
      };

      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3 WHERE timestamp >= $1 ORDER BY timestamp ASC, id ASC LIMIT $2'),
        [new Date('2023-01-01T00:00:00Z'), 50]
      );
      expect(result).toEqual({ items: logs });
    });

    it('should apply level_json filter', async () => {
      const logs = [
        { id: 1, hostname: 'localhost', pid: 1234, source: 'test', timestamp: new Date(), level: 'info', message: 'test message' }
      ];
      const filters = {
        level_json: [
          { source: 'test', level: 'info' },
          { source: 'another_test', level: 'warn' }
        ],
        limit: 50
      };
      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3'),
        expect.arrayContaining(['test', 'info', 'another_test', 'warn', 50])
      );
      expect(result).toEqual({ items: logs });
    });

    it('should reverse the result if shouldReverse is true', async () => {
      const logs = [
        { id: 1, hostname: 'localhost', pid: 1234, source: 'test', timestamp: new Date(), level: 'info', message: 'test message' },
        { id: 2, hostname: 'localhost', pid: 1234, source: 'test', timestamp: new Date(), level: 'info', message: 'another message' }
      ];
      const filters = {
        lt_id: 10,
        limit: 50
      };
      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3 WHERE id < $1 ORDER BY id DESC LIMIT $2'),
        [10, 50]
      );
      expect(result.items).toEqual(logs.reverse());
    });

    it('should retrieve logs filtered by a single hostname', async () => {
      const mockLogs = [
        {
          id: 1,
          hostname: 'localhost',
          pid: 1234,
          source: 'test',
          timestamp: new Date(),
          level: 'info',
          message: 'Test message 1',
          errsole_id: 'err1'
        }
      ];
      const filters = {
        hostnames: ['localhost'],
        limit: 50
      };
      poolMock.query.mockResolvedValueOnce({ rows: mockLogs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('WHERE hostname = ANY($1)'),
        [['localhost'], 50]
      );
      expect(result).toEqual({ items: mockLogs });
    });

    it('should retrieve logs filtered by multiple hostnames', async () => {
      const mockLogs = [
        {
          id: 2,
          hostname: 'server1',
          pid: 5678,
          source: 'test',
          timestamp: new Date(),
          level: 'error',
          message: 'Test message 2',
          errsole_id: 'err2'
        },
        {
          id: 3,
          hostname: 'server2',
          pid: 9101,
          source: 'test',
          timestamp: new Date(),
          level: 'warn',
          message: 'Test message 3',
          errsole_id: 'err3'
        }
      ];
      const filters = {
        hostnames: ['server1', 'server2'],
        limit: 50
      };
      poolMock.query.mockResolvedValueOnce({ rows: mockLogs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('WHERE hostname = ANY($1)'),
        [['server1', 'server2'], 50]
      );
      expect(result).toEqual({ items: mockLogs });
    });

    it('should ignore the hostnames filter if the array is empty', async () => {
      const mockLogs = [
        {
          id: 4,
          hostname: 'server3',
          pid: 1121,
          source: 'test',
          timestamp: new Date(),
          level: 'info',
          message: 'Test message 4',
          errsole_id: 'err4'
        }
      ];
      const filters = {
        hostnames: [],
        limit: 50
      };
      poolMock.query.mockResolvedValueOnce({ rows: mockLogs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.not.stringContaining('WHERE hostname = ANY'),
        [50]
      );
      expect(result).toEqual({ items: mockLogs });
    });

    it('should retrieve all logs if hostnames filter is not provided', async () => {
      const mockLogs = [
        {
          id: 5,
          hostname: 'server4',
          pid: 3141,
          source: 'test',
          timestamp: new Date(),
          level: 'debug',
          message: 'Test message 5',
          errsole_id: 'err5'
        }
      ];
      const filters = {
        limit: 50
      };
      poolMock.query.mockResolvedValueOnce({ rows: mockLogs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.not.stringContaining('WHERE hostname = ANY'),
        [50]
      );
      expect(result).toEqual({ items: mockLogs });
    });

    it('should handle errors during log retrieval', async () => {
      poolMock.query.mockRejectedValueOnce(new Error('Query error'));

      await expect(errsolePostgres.getLogs()).rejects.toThrow('Query error');

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3'),
        [100]
      );
    });

    it('should retrieve logs using level_json filter', async () => {
      const filters = {
        level_json: [
          { source: 'source1', level: 'info' },
          { source: 'source2', level: 'error' }
        ],
        limit: 50
      };

      const logs = [
        { id: 1, hostname: 'localhost', pid: 1234, source: 'source1', level: 'info', message: 'Log 1' },
        { id: 2, hostname: 'localhost', pid: 5678, source: 'source2', level: 'error', message: 'Log 2' }
      ];

      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolQuerySpy).toHaveBeenCalledWith(
        expect.stringContaining(
          'SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3 WHERE ('
        ),
        expect.arrayContaining(['source1', 'info', 'source2', 'error', 50])
      );
      expect(result.items).toEqual(logs);
    });

    it('should retrieve logs using errsole_id filter', async () => {
      const filters = {
        errsole_id: 123,
        limit: 50
      };

      const logs = [
        { id: 1, hostname: 'localhost', pid: 1234, source: 'source1', level: 'info', message: 'Log 1', errsole_id: 123 }
      ];

      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolQuerySpy).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3 WHERE ('),
        expect.arrayContaining([123, 50])
      );
      expect(result.items).toEqual(logs);
    });

    it('should retrieve logs using both level_json and errsole_id filters', async () => {
      const filters = {
        level_json: [
          { source: 'source1', level: 'info' }
        ],
        errsole_id: 123,
        limit: 50
      };

      const logs = [
        { id: 1, hostname: 'localhost', pid: 1234, source: 'source1', level: 'info', message: 'Log 1', errsole_id: 123 }
      ];

      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolQuerySpy).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3 WHERE ('),
        expect.arrayContaining(['source1', 'info', 123, 50])
      );
      expect(result.items).toEqual(logs);
    });

    it('should return empty array if no logs are found with level_json and errsole_id filters', async () => {
      const filters = {
        level_json: [
          { source: 'source1', level: 'info' }
        ],
        errsole_id: 123,
        limit: 50
      };

      poolMock.query.mockResolvedValueOnce({ rows: [] });

      const result = await errsolePostgres.getLogs(filters);

      expect(poolQuerySpy).toHaveBeenCalledWith(
        expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v3 WHERE ('),
        expect.arrayContaining(['source1', 'info', 123, 50])
      );
      expect(result.items).toEqual([]);
    });
  });

  describe('#getUserCount', () => {
    it('should successfully retrieve the user count', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [{ count: '5' }] });

      const result = await errsolePostgres.getUserCount();

      expect(poolMock.query).toHaveBeenCalledWith('SELECT COUNT(*) as count FROM errsole_users');
      expect(result).toEqual({ count: 5 });
    });

    it('should handle errors during query execution', async () => {
      const error = new Error('Query error');
      poolMock.query.mockRejectedValueOnce(error);

      await expect(errsolePostgres.getUserCount()).rejects.toThrow('Query error');

      expect(poolMock.query).toHaveBeenCalledWith('SELECT COUNT(*) as count FROM errsole_users');
    });
  });

  describe('#getMeta', () => {
    it('should successfully retrieve metadata for a given log ID', async () => {
      const logMeta = { id: 1, meta: 'test meta data' };
      poolMock.query.mockResolvedValueOnce({ rows: [logMeta] });

      const result = await errsolePostgres.getMeta(1);

      expect(poolMock.query).toHaveBeenCalledWith('SELECT id, meta FROM errsole_logs_v3 WHERE id = $1', [1]);
      expect(result).toEqual({ item: logMeta });
    });

    it('should throw an error if the log entry is not found', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [] });

      await expect(errsolePostgres.getMeta(999)).rejects.toThrow('Log entry not found.');

      expect(poolMock.query).toHaveBeenCalledWith('SELECT id, meta FROM errsole_logs_v3 WHERE id = $1', [999]);
    });

    it('should handle errors during query execution', async () => {
      const error = new Error('Query error');
      poolMock.query.mockRejectedValueOnce(error);

      await expect(errsolePostgres.getMeta(1)).rejects.toThrow('Query error');

      expect(poolMock.query).toHaveBeenCalledWith('SELECT id, meta FROM errsole_logs_v3 WHERE id = $1', [1]);
    });
  });

  describe('#deleteUser', () => {
    it('should delete user by id', async () => {
      poolMock.query.mockResolvedValue({ rowCount: 1 });

      await errsolePostgres.deleteUser(1);

      expect(poolMock.query).toHaveBeenCalledWith('DELETE FROM errsole_users WHERE id = $1', [1]);
    });

    it('should throw error if user not found', async () => {
      poolMock.query.mockResolvedValue({ rowCount: 0 });

      await expect(errsolePostgres.deleteUser(1)).rejects.toThrow(new Error('User not found.'));
    });

    it('should throw error if no id is provided', async () => {
      await expect(errsolePostgres.deleteUser()).rejects.toThrow('User ID is required.');
    });
  });

  describe('#deleteExpiredLogs', () => {
    let getConfigSpy;
    let poolQuerySpy;
    let setTimeoutSpy;

    beforeEach(() => {
      getConfigSpy = jest.spyOn(errsolePostgres, 'getConfig').mockResolvedValue({ item: { key: 'logsTTL', value: '2592000000' } });
      poolQuerySpy = jest.spyOn(poolMock, 'query');
      setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback) => callback());
      errsolePostgres.deleteExpiredLogsRunning = false; // Reset the flag before each test
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should delete expired logs based on TTL', async () => {
      poolQuerySpy
        .mockResolvedValueOnce({ rowCount: 1000 }) // First query response
        .mockResolvedValueOnce({ rowCount: 0 }); // Second query response

      await errsolePostgres.deleteExpiredLogs();

      expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
      expect(poolQuerySpy).toHaveBeenCalledWith(expect.any(String), [expect.any(String)]);
      expect(setTimeoutSpy).toHaveBeenCalled();
    });

    it('should handle error in pool.query', async () => {
      const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
      poolQuerySpy.mockRejectedValue(new Error('Test error'));

      await errsolePostgres.deleteExpiredLogs();

      expect(consoleErrorSpy).toHaveBeenCalledWith(new Error('Test error'));
      consoleErrorSpy.mockRestore();
    });

    it('should handle invalid TTL from config', async () => {
      getConfigSpy.mockResolvedValueOnce({ item: { key: 'logsTTL', value: 'invalid' } });
      poolQuerySpy
        .mockResolvedValueOnce({ rowCount: 1000 }) // First query response
        .mockResolvedValueOnce({ rowCount: 0 }); // Second query response

      await errsolePostgres.deleteExpiredLogs();

      expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
      expect(poolQuerySpy).toHaveBeenCalledWith(expect.any(String), [expect.any(String)]);
      expect(setTimeoutSpy).toHaveBeenCalled();
    });

    it('should use default TTL if config is not found', async () => {
      getConfigSpy.mockResolvedValueOnce({ item: null });
      poolQuerySpy
        .mockResolvedValueOnce({ rowCount: 1000 }) // First query response
        .mockResolvedValueOnce({ rowCount: 0 }); // Second query response

      await errsolePostgres.deleteExpiredLogs();

      expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
      expect(poolQuerySpy).toHaveBeenCalledWith(expect.any(String), [expect.any(String)]);
      expect(setTimeoutSpy).toHaveBeenCalled();
    });

    it('should reset deleteExpiredLogsRunning flag after execution', async () => {
      poolQuerySpy.mockResolvedValueOnce({ rowCount: 0 });

      await errsolePostgres.deleteExpiredLogs();

      expect(errsolePostgres.deleteExpiredLogsRunning).toBe(false);
    });
  });

  describe('#setConfig', () => {
    let getConfigSpy;

    beforeEach(() => {
      getConfigSpy = jest.spyOn(errsolePostgres, 'getConfig').mockResolvedValue({ item: { key: 'testKey', value: 'testValue' } });
      poolMock.query.mockClear(); // Clear any previous calls
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should insert a new configuration if it does not exist', async () => {
      poolMock.query.mockResolvedValueOnce({ rowCount: 1 });
      const config = await errsolePostgres.setConfig('newKey', 'newValue');

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringMatching(/INSERT INTO errsole_config \(key, value\) VALUES \(\$1, \$2\)\s+ON CONFLICT \(key\) DO UPDATE SET value = EXCLUDED.value/),
        ['newKey', 'newValue']
      );
      expect(getConfigSpy).toHaveBeenCalledWith('newKey');
      expect(config).toEqual({ item: { key: 'testKey', value: 'testValue' } });
    });

    it('should update an existing configuration', async () => {
      poolMock.query.mockResolvedValueOnce({ rowCount: 1 });
      const config = await errsolePostgres.setConfig('existingKey', 'newValue');

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringMatching(/INSERT INTO errsole_config \(key, value\) VALUES \(\$1, \$2\)\s+ON CONFLICT \(key\) DO UPDATE SET value = EXCLUDED.value/),
        ['existingKey', 'newValue']
      );
      expect(getConfigSpy).toHaveBeenCalledWith('existingKey');
      expect(config).toEqual({ item: { key: 'testKey', value: 'testValue' } });
    });

    it('should handle errors in setting configuration', async () => {
      poolMock.query.mockRejectedValueOnce(new Error('Query error'));

      await expect(errsolePostgres.setConfig('errorKey', 'errorValue')).rejects.toThrow('Query error');

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringMatching(/INSERT INTO errsole_config \(key, value\) VALUES \(\$1, \$2\)\s+ON CONFLICT \(key\) DO UPDATE SET value = EXCLUDED.value/),
        ['errorKey', 'errorValue']
      );
    });

    it('should handle errors in getting configuration after setting it', async () => {
      poolMock.query.mockResolvedValueOnce({ rowCount: 1 });
      getConfigSpy.mockRejectedValueOnce(new Error('Get config error'));

      await expect(errsolePostgres.setConfig('key', 'value')).rejects.toThrow('Get config error');

      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringMatching(/INSERT INTO errsole_config \(key, value\) VALUES \(\$1, \$2\)\s+ON CONFLICT \(key\) DO UPDATE SET value = EXCLUDED.value/),
        ['key', 'value']
      );
      expect(getConfigSpy).toHaveBeenCalledWith('key');
    });
  });

  // describe('#ensureLogsTTL', () => {
  //   let getConfigSpy;
  //   let setConfigSpy; it('should handle query errors during user password update', async () => {
  //     const user = { id: 1, name: 'test', email: 'test@example.com', hashed_password: 'hashedPassword', role: 'admin' };
  //     poolMock.query
  //       .mockResolvedValueOnce({ rows: [user] }) // First query response
  //       .mockRejectedValueOnce(new Error('Query error')); // Second query response
  //     bcrypt.compare.mockResolvedValue(true);
  //     bcrypt.hash.mockResolvedValue('newHashedPassword');

  //     await expect(errsolePostgres.updatePassword('test@example.com', 'password', 'newPassword')).rejects.toThrow('Query error');
  //   });

  //   beforeEach(() => {
  //     getConfigSpy = jest.spyOn(errsolePostgres, 'getConfig');
  //     setConfigSpy = jest.spyOn(errsolePostgres, 'setConfig').mockResolvedValue({ item: { key: 'logsTTL', value: '2592000000' } });
  //   });

  //   afterEach(() => {
  //     jest.clearAllMocks();
  //   });

  //   it('should set default logsTTL if config does not exist', async () => {
  //     getConfigSpy.mockResolvedValueOnce({ item: null });

  //     await errsolePostgres.ensureLogsTTL();

  //     expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
  //     expect(setConfigSpy).toHaveBeenCalledWith('logsTTL', '2592000000');
  //   });

  //   it('should not set logsTTL if config already exists', async () => {
  //     getConfigSpy.mockResolvedValueOnce({ item: { key: 'logsTTL', value: '2592000000' } });

  //     await errsolePostgres.ensureLogsTTL();

  //     expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
  //     expect(setConfigSpy).not.toHaveBeenCalled();
  //   });

  //   it('should handle errors in getting configuration', async () => {
  //     getConfigSpy.mockRejectedValueOnce(new Error('Query error'));

  //     await expect(errsolePostgres.ensureLogsTTL()).rejects.toThrow('Query error');

  //     expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
  //     expect(setConfigSpy).not.toHaveBeenCalled();
  //   });

  //   it('should handle errors in setting configuration', async () => {
  //     getConfigSpy.mockResolvedValueOnce({ item: null });
  //     setConfigSpy.mockRejectedValueOnce(new Error('Query error'));

  //     await expect(errsolePostgres.ensureLogsTTL()).rejects.toThrow('Query error');

  //     expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
  //     expect(setConfigSpy).toHaveBeenCalledWith('logsTTL', '2592000000');
  //   });
  // });

  describe('#getHostnames', () => {
    let poolQuerySpy;

    beforeEach(() => {
      poolQuerySpy = jest.spyOn(poolMock, 'query');
      poolMock.query.mockClear(); // Clear any previous calls
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should retrieve a sorted list of distinct hostnames', async () => {
      const mockHostnames = [
        { hostname: 'server3.example.com' },
        { hostname: 'server1.example.com' },
        { hostname: 'server2.example.com' }
      ];
      poolMock.query.mockImplementationOnce((query, callback) => {
        callback(null, { rows: mockHostnames });
      });

      const result = await errsolePostgres.getHostnames();

      expect(poolQuerySpy).toHaveBeenCalledWith(expect.stringContaining('SELECT DISTINCT hostname'), expect.any(Function));
      expect(result).toEqual({ items: ['server1.example.com', 'server2.example.com', 'server3.example.com'] });
    });

    it('should return an empty array if no hostnames are found', async () => {
      poolMock.query.mockImplementationOnce((query, callback) => {
        callback(null, { rows: [] });
      });

      const result = await errsolePostgres.getHostnames();

      expect(poolQuerySpy).toHaveBeenCalledWith(expect.stringContaining('SELECT DISTINCT hostname'), expect.any(Function));
      expect(result).toEqual({ items: [] });
    });

    it('should handle errors during the query execution', async () => {
      const error = new Error('Database query failed');
      poolMock.query.mockImplementationOnce((query, callback) => {
        callback(error, null);
      });

      await expect(errsolePostgres.getHostnames()).rejects.toThrow('Database query failed');

      expect(poolQuerySpy).toHaveBeenCalledWith(expect.stringContaining('SELECT DISTINCT hostname'), expect.any(Function));
    });

    it('should correctly sort hostnames with special characters and varying cases', async () => {
      const mockHostnames = [
        { hostname: 'Server-2.Example.com' },
        { hostname: 'server-10.example.com' },
        { hostname: 'server-1.example.com' },
        { hostname: 'SERVER-3.EXAMPLE.COM' }
      ];
      poolMock.query.mockImplementationOnce((query, callback) => {
        callback(null, { rows: mockHostnames });
      });

      const result = await errsolePostgres.getHostnames();

      expect(result).toEqual({
        items: [
          'SERVER-3.EXAMPLE.COM',
          'Server-2.Example.com',
          'server-1.example.com',
          'server-10.example.com'
        ]
      });
    });

    // Optional: Performance Test (Note: Jest is not ideal for performance testing, but here's a basic example)
    it('should retrieve hostnames efficiently with a large dataset', async () => {
      const largeNumber = 1000;
      const mockHostnames = Array.from({ length: largeNumber }, (_, i) => ({ hostname: `server${i}.example.com` }));
      poolMock.query.mockImplementationOnce((query, callback) => {
        callback(null, { rows: mockHostnames });
      });

      const result = await errsolePostgres.getHostnames();

      expect(result.items.length).toBe(largeNumber);
      expect(result.items[0]).toBe('server0.example.com');
      expect(result.items[largeNumber - 1]).toBe(`server${largeNumber - 1}.example.com`);
    });
  });

  describe('#insertNotificationItem', () => {
    it('should insert a notification item and return the previous item and today\'s notification count', async () => {
      const notification = {
        errsole_id: 'test-errsole-id',
        hostname: 'localhost',
        hashed_message: 'hashedMessage'
      };

      // Mock responses for all queries including BEGIN and COMMIT
      clientMock.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ // fetchPreviousNotificationQuery
          rows: [{ id: 1, hostname: 'localhost', hashed_message: 'hashedMessage', created_at: new Date() }]
        })
        .mockResolvedValueOnce({}) // insertNotificationQuery
        .mockResolvedValueOnce({ // countTodayNotificationsQuery
          rows: [{ notificationcount: '3' }] // PostgreSQL returns counts as strings
        })
        .mockResolvedValueOnce({}) // COMMIT
        .mockResolvedValueOnce({}); // Additional Query (e.g., SELECT NOW())

      const result = await errsolePostgres.insertNotificationItem(notification);

      // Expect 6 queries: BEGIN, fetchPrevious, insert, count, COMMIT, additional
      expect(clientMock.query).toHaveBeenCalledTimes(6);

      expect(result).toEqual({
        previousNotificationItem: {
          id: 1,
          hostname: 'localhost',
          hashed_message: 'hashedMessage',
          created_at: expect.any(Date)
        },
        todayNotificationCount: '3' // Keeping as string to match PostgreSQL return type
      });
    });

    it('should return undefined previous item and todayNotificationCount as "1" when no previous notification exists', async () => {
      const notification = {
        errsole_id: 'test-errsole-id',
        hostname: 'localhost',
        hashed_message: 'newHashedMessage'
      };

      clientMock.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ // fetchPreviousNotificationQuery returns no previous items
          rows: []
        })
        .mockResolvedValueOnce({}) // insertNotificationQuery
        .mockResolvedValueOnce({ // countTodayNotificationsQuery
          rows: [{ notificationcount: '1' }]
        })
        .mockResolvedValueOnce({}) // COMMIT
        .mockResolvedValueOnce({}); // Additional Query

      const result = await errsolePostgres.insertNotificationItem(notification);

      expect(result).toEqual({
        previousNotificationItem: undefined, // Accepting undefined instead of null
        todayNotificationCount: '1'
      });
    });

    it('should handle a database error by rolling back the transaction and throw an error', async () => {
      const notification = {
        errsole_id: 'test-errsole-id',
        hostname: 'localhost',
        hashed_message: 'errorHashedMessage'
      };

      // Mocking the query method
      clientMock.query.mockImplementation((query) => {
        if (query.includes('INSERT INTO errsole_notifications')) {
          return Promise.reject(new Error('Insert error')); // Simulate insert failure
        }
        if (query.includes('ROLLBACK')) {
          return Promise.resolve({});
        }
        if (query.includes('BEGIN')) {
          return Promise.resolve({});
        }
        if (query.includes('SELECT * FROM errsole_notifications')) {
          return Promise.resolve({ rows: [] }); // No previous notification
        }
        // For other queries like COUNT or SELECT NOW()
        return Promise.resolve({});
      });

      // Expect the method to throw an error
      await expect(errsolePostgres.insertNotificationItem(notification)).rejects.toThrow('Insert error');

      // Verify that BEGIN was called
      expect(clientMock.query).toHaveBeenCalledWith('BEGIN');

      // Verify that INSERT was attempted with correct parameter order
      expect(clientMock.query).toHaveBeenCalledWith(
        expect.stringContaining('INSERT INTO errsole_notifications'),
        [notification.errsole_id, notification.hostname, notification.hashed_message] // Corrected order
      );

      // Verify that ROLLBACK was called after the insert failed
      expect(clientMock.query).toHaveBeenCalledWith('ROLLBACK');

      // Ensure that the client was released
      expect(clientMock.release).toHaveBeenCalled();
    });

    it('should release the client after operation', async () => {
      const notification = {
        errsole_id: 'test-errsole-id',
        hostname: 'localhost',
        hashed_message: 'hashedMessage'
      };

      // Mock successful transaction with all queries
      clientMock.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ // fetchPreviousNotificationQuery
          rows: [{ id: 1, hostname: 'localhost', hashed_message: 'hashedMessage', created_at: new Date() }]
        })
        .mockResolvedValueOnce({}) // insertNotificationQuery
        .mockResolvedValueOnce({ // countTodayNotificationsQuery
          rows: [{ notificationcount: '3' }]
        })
        .mockResolvedValueOnce({}) // COMMIT
        .mockResolvedValueOnce({}); // Additional Query

      await errsolePostgres.insertNotificationItem(notification);

      expect(clientMock.release).toHaveBeenCalled();
    });
  });

  describe('#deleteExpiredNotificationItems', () => {
    let getConfigSpy;
    let poolQuerySpy;
    let setTimeoutSpy;

    beforeEach(() => {
      getConfigSpy = jest.spyOn(errsolePostgres, 'getConfig').mockResolvedValue({ item: { key: 'logsTTL', value: '2592000000' } });
      poolQuerySpy = jest.spyOn(poolMock, 'query');
      setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback) => callback());
      errsolePostgres.deleteExpiredNotificationItemsRunning = false; // Reset the flag before each test
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should delete expired notification items based on TTL configuration', async () => {
      poolQuerySpy
        .mockResolvedValueOnce({ rowCount: 1000 }) // First batch of deletions
        .mockResolvedValueOnce({ rowCount: 0 }); // Second batch indicating no more items

      await errsolePostgres.deleteExpiredNotificationItems();

      expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
      expect(poolQuerySpy).toHaveBeenCalledWith(expect.any(String), [expect.any(String)]);
      expect(setTimeoutSpy).toHaveBeenCalled();
    });

    it('should handle errors during deletion process and reset running flag', async () => {
      const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
      poolQuerySpy.mockRejectedValueOnce(new Error('Query error'));

      await errsolePostgres.deleteExpiredNotificationItems();

      expect(consoleErrorSpy).toHaveBeenCalledWith(new Error('Query error'));
      expect(errsolePostgres.deleteExpiredNotificationItemsRunning).toBe(false);
      consoleErrorSpy.mockRestore();
    });

    it('should use default TTL if configuration key is not found', async () => {
      getConfigSpy.mockResolvedValueOnce({ item: null });
      poolQuerySpy
        .mockResolvedValueOnce({ rowCount: 1000 }) // First batch
        .mockResolvedValueOnce({ rowCount: 0 }); // No more items

      await errsolePostgres.deleteExpiredNotificationItems();

      expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
      expect(poolQuerySpy).toHaveBeenCalledWith(expect.any(String), [expect.any(String)]);
      expect(setTimeoutSpy).toHaveBeenCalled();
    });

    it('should reset deleteExpiredNotificationItemsRunning flag after execution', async () => {
      poolQuerySpy.mockResolvedValueOnce({ rowCount: 0 });

      await errsolePostgres.deleteExpiredNotificationItems();

      expect(errsolePostgres.deleteExpiredNotificationItemsRunning).toBe(false);
    });
  });

  describe('#deleteAllLogs', () => {
    let poolQuerySpy;

    beforeEach(() => {
      poolQuerySpy = jest.spyOn(poolMock, 'query');
      poolMock.query.mockClear(); // Clear any previous calls
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should delete all logs and reset the table identity', async () => {
      poolMock.query.mockResolvedValueOnce({});

      const result = await errsolePostgres.deleteAllLogs();

      expect(poolQuerySpy).toHaveBeenCalledWith('TRUNCATE TABLE errsole_logs_v3 RESTART IDENTITY CASCADE');
      expect(result).toEqual({});
    });

    it('should throw an error if the query fails', async () => {
      const error = new Error('Query error');
      poolMock.query.mockRejectedValueOnce(error);

      await expect(errsolePostgres.deleteAllLogs()).rejects.toThrow('Query error');

      expect(poolQuerySpy).toHaveBeenCalledWith('TRUNCATE TABLE errsole_logs_v3 RESTART IDENTITY CASCADE');
    });
  });

  describe('#searchLogs', () => {
    let poolQuerySpy;

    beforeEach(() => {
      poolQuerySpy = jest.spyOn(poolMock, 'query');
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should retrieve logs with no filters and search terms', async () => {
      const logs = [
        { id: 1, hostname: 'localhost', pid: 1234, source: 'test', timestamp: new Date(), level: 'info', message: 'test message' }
      ];
      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.searchLogs();

      expect(poolMock.query).toHaveBeenCalledWith(expect.stringContaining('SELECT id, hostname, pid, source, timestamp, level, message,errsole_id'), expect.any(Array));
      expect(result.items).toEqual(logs.reverse());
    });

    it('should apply search terms filter', async () => {
      const logs = [{ id: 1, message: 'error occurred' }];
      const searchTerms = ['error', 'occurred'];
      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.searchLogs(searchTerms);

      expect(poolMock.query).toHaveBeenCalledWith(expect.stringContaining('message_tsv @@ phraseto_tsquery'), expect.arrayContaining(searchTerms));
      expect(result.items).toEqual(logs.reverse());
    });

    it('should apply hostnames filter', async () => {
      const logs = [{ id: 2, hostname: 'server1' }];
      const filters = { hostnames: ['server1', 'server2'] };
      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.searchLogs([], filters);

      expect(poolMock.query).toHaveBeenCalledWith(expect.stringContaining('hostname = ANY'), expect.arrayContaining([['server1', 'server2']]));
      expect(result.items).toEqual(logs.reverse());
    });

    it('should apply level_json and errsole_id filters', async () => {
      const logs = [{ id: 3, source: 'app', level: 'error', errsole_id: 123 }];
      const filters = {
        level_json: [{ source: 'app', level: 'error' }],
        errsole_id: 123
      };
      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.searchLogs([], filters);

      expect(poolMock.query).toHaveBeenCalledWith(expect.stringContaining('(source = $'), expect.arrayContaining(['app', 'error', 123]));
      expect(result.items).toEqual(logs.reverse());
    });

    it('should apply lt_id filter and order logs correctly', async () => {
      const logs = [{ id: 4, message: 'test log' }];
      const filters = { lt_id: 10 };
      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.searchLogs([], filters);

      expect(poolMock.query).toHaveBeenCalledWith(expect.stringContaining('id < $'), expect.arrayContaining([10]));
      expect(result.items).toEqual(logs.reverse());
    });

    it('should apply gt_id filter and order logs correctly', async () => {
      const logs = [{ id: 5, message: 'new log' }];
      const filters = { gt_id: 5 };
      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.searchLogs([], filters);

      expect(poolMock.query).toHaveBeenCalledWith(expect.stringContaining('id > $'), expect.arrayContaining([5]));
      expect(result.items).toEqual(logs);
    });

    it('should apply timestamp filters and adjust missing gte_timestamp or lte_timestamp', async () => {
      const logs = [{ id: 6, timestamp: new Date() }];
      const filters = { lte_timestamp: new Date('2023-01-01T00:00:00Z') };
      const expectedGteTimestamp = new Date(filters.lte_timestamp.getTime() - 24 * 60 * 60 * 1000);

      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.searchLogs([], filters);

      expect(poolMock.query).toHaveBeenCalledWith(expect.stringContaining('timestamp <= $'), expect.arrayContaining([filters.lte_timestamp, expectedGteTimestamp]));
      expect(result.items).toEqual(logs.reverse());
    });

    it('should apply gte_timestamp and auto-set lte_timestamp if missing', async () => {
      const logs = [{ id: 7, timestamp: new Date() }];
      const filters = { gte_timestamp: new Date('2023-01-01T00:00:00Z') };
      const expectedLteTimestamp = new Date(filters.gte_timestamp.getTime() + 24 * 60 * 60 * 1000);

      poolMock.query.mockResolvedValueOnce({ rows: logs });

      const result = await errsolePostgres.searchLogs([], filters);

      expect(poolMock.query).toHaveBeenCalledWith(expect.stringContaining('timestamp >= $'), expect.arrayContaining([filters.gte_timestamp, expectedLteTimestamp]));
      expect(result.items).toEqual(logs.reverse());
    });

    it('should handle errors during search query execution', async () => {
      poolMock.query.mockRejectedValueOnce(new Error('Query error'));

      await expect(errsolePostgres.searchLogs()).rejects.toThrow('Query error');
    });
  });

  describe('#createUser', () => {
    it('should create a new user successfully', async () => {
      const user = { name: 'John', email: 'john@example.com', password: 'password123', role: 'admin' };
      const hashedPassword = 'hashedPassword';

      bcrypt.hash.mockResolvedValueOnce(hashedPassword);
      poolMock.query.mockResolvedValueOnce({ rows: [{ id: 1 }] });

      const result = await errsolePostgres.createUser(user);

      expect(bcrypt.hash).toHaveBeenCalledWith(user.password, 10);
      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('INSERT INTO'),
        expect.arrayContaining([user.name, user.email, hashedPassword, user.role])
      );
      expect(result).toEqual({ item: { id: 1, name: user.name, email: user.email, role: user.role } });
    });

    it('should throw an error if database query fails unexpectedly', async () => {
      const user = { name: 'Jane', email: 'jane@example.com', password: 'securepass', role: 'user' };
      const hashedPassword = 'hashedPassword';

      bcrypt.hash.mockResolvedValueOnce(hashedPassword);
      const originalQueryMock = poolMock.query;
      poolMock.query = jest.fn().mockImplementation((query, values) => {
        if (query.includes('INSERT INTO')) {
          return Promise.reject(new Error('Database error'));
        }
        return originalQueryMock(query, values);
      });

      await expect(errsolePostgres.createUser(user)).rejects.toThrow('Database error');

      expect(bcrypt.hash).toHaveBeenCalledWith(user.password, 10);
      expect(poolMock.query).toHaveBeenCalledWith(
        expect.stringContaining('INSERT INTO'),
        expect.arrayContaining([user.name, user.email, hashedPassword, user.role])
      );
      poolMock.query = originalQueryMock;
    });
  });

  describe('#ensureLogsTTL', () => {
    let getConfigSpy, setConfigSpy;

    beforeEach(() => {
      getConfigSpy = jest.spyOn(errsolePostgres, 'getConfig').mockResolvedValue({ item: { key: 'logsTTL', value: '2592000000' } });
      setConfigSpy = jest.spyOn(errsolePostgres, 'setConfig').mockResolvedValue({ item: { key: 'logsTTL', value: '2592000000' } });
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should set logsTTL to default if it does not exist', async () => {
      getConfigSpy.mockResolvedValueOnce({ item: null });

      await errsolePostgres.ensureLogsTTL();

      expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
      expect(setConfigSpy).toHaveBeenCalledWith('logsTTL', (30 * 24 * 60 * 60 * 1000).toString());
    });

    it('should not update logsTTL if it already exists', async () => {
      getConfigSpy.mockResolvedValueOnce({ item: { key: 'logsTTL', value: '2592000000' } });

      await errsolePostgres.ensureLogsTTL();

      expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
      expect(setConfigSpy).not.toHaveBeenCalled(); // ✅ Now this should pass
    });

    it('should handle errors in getConfig', async () => {
      getConfigSpy.mockRejectedValueOnce(new Error('Query error'));

      await expect(errsolePostgres.ensureLogsTTL()).rejects.toThrow('Query error');

      expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
      expect(setConfigSpy).not.toHaveBeenCalled(); // ✅ Now this should pass
    });

    it('should handle errors in setConfig when logsTTL does not exist', async () => {
      getConfigSpy.mockResolvedValueOnce({ item: null });
      setConfigSpy.mockRejectedValueOnce(new Error('Insert error'));

      await expect(errsolePostgres.ensureLogsTTL()).rejects.toThrow('Insert error');

      expect(getConfigSpy).toHaveBeenCalledWith('logsTTL');
      expect(setConfigSpy).toHaveBeenCalledWith('logsTTL', (30 * 24 * 60 * 60 * 1000).toString());
    });
  });

  describe('#flushLogs', () => {
    let fakeRelease;
    let fakeClient;

    beforeEach(() => {
      // Set the logs table and clear pendingLogs and reset the connection flag.
      errsolePostgres.logsTable = 'errsole_logs_v3';
      errsolePostgres.pendingLogs = [];
      errsolePostgres.isConnectionInProgress = false;

      // Reset any previous mocks for pool.connect
      jest.spyOn(errsolePostgres.pool, 'connect').mockRestore();
    });

    it('should return {} immediately if no logs to flush', async () => {
      errsolePostgres.pendingLogs = []; // Ensure no logs are pending
      const result = await errsolePostgres.flushLogs();
      expect(result).toEqual({});
      // Since there are no logs, pool.connect should not be called.
      expect(errsolePostgres.pool.connect).not.toHaveBeenCalled();
    });

    it('should flush logs and resolve {} when logs are present and query succeeds', async () => {
      const sampleLog = {
        timestamp: new Date('2023-01-01T00:00:00Z'),
        hostname: 'localhost',
        pid: 1234,
        source: 'test',
        level: 'info',
        message: 'test message',
        meta: 'meta data',
        errsole_id: 'err1'
      };
      errsolePostgres.pendingLogs = [sampleLog];

      fakeRelease = jest.fn();
      fakeClient = {
        query: jest.fn((query, queryParams, callback) => {
          // Simulate a successful query execution
          callback(null);
        })
      };

      // Override pool.connect to simulate a callback-style connection.
      jest.spyOn(errsolePostgres.pool, 'connect').mockImplementation((callback) => {
        callback(null, fakeClient, fakeRelease);
      });

      const result = await errsolePostgres.flushLogs();

      // Verify that pendingLogs is cleared after flush.
      expect(errsolePostgres.pendingLogs).toEqual([]);
      // Check that the client.query was called with a query that contains the INSERT statement.
      expect(fakeClient.query).toHaveBeenCalledWith(
        expect.stringContaining('INSERT INTO errsole_logs_v3'),
        expect.any(Array),
        expect.any(Function)
      );
      // Verify that the client was released.
      expect(fakeRelease).toHaveBeenCalled();
      // Ensure the flushLogs method resolved with an empty object.
      expect(result).toEqual({});
    });

    it('should wait if isConnectionInProgress is true and then flush logs', async () => {
      // Use fake timers to simulate waiting.
      jest.useFakeTimers();
      errsolePostgres.isConnectionInProgress = true;

      const sampleLog = {
        timestamp: new Date('2023-01-01T00:00:00Z'),
        hostname: 'localhost',
        pid: 1234,
        source: 'test',
        level: 'info',
        message: 'test message',
        meta: 'meta data',
        errsole_id: 'err1'
      };
      errsolePostgres.pendingLogs = [sampleLog];

      fakeRelease = jest.fn();
      fakeClient = {
        query: jest.fn((query, queryParams, callback) => {
          callback(null);
        })
      };

      jest.spyOn(errsolePostgres.pool, 'connect').mockImplementation((callback) => {
        callback(null, fakeClient, fakeRelease);
      });

      // Start flushLogs. It should wait until isConnectionInProgress is false.
      const flushPromise = errsolePostgres.flushLogs();

      // After 150ms, mark connection as no longer in progress.
      setTimeout(() => {
        errsolePostgres.isConnectionInProgress = false;
      }, 150);

      // Fast-forward timers.
      jest.advanceTimersByTime(200);
      const result = await flushPromise;

      // Restore real timers.
      jest.useRealTimers();

      expect(errsolePostgres.pendingLogs).toEqual([]);
      expect(fakeClient.query).toHaveBeenCalled();
      expect(fakeRelease).toHaveBeenCalled();
      expect(result).toEqual({});
    });

    it('should reject if pool.connect returns an error', async () => {
      const sampleLog = {
        timestamp: new Date('2023-01-01T00:00:00Z'),
        hostname: 'localhost',
        pid: 1234,
        source: 'test',
        level: 'info',
        message: 'test message',
        meta: 'meta data',
        errsole_id: 'err1'
      };
      errsolePostgres.pendingLogs = [sampleLog];

      const connectError = new Error('Connection failed');
      jest.spyOn(errsolePostgres.pool, 'connect').mockImplementation((callback) => {
        callback(connectError);
      });

      await expect(errsolePostgres.flushLogs()).rejects.toThrow('Connection failed');
    });

    it('should reject if client.query returns an error', async () => {
      const sampleLog = {
        timestamp: new Date('2023-01-01T00:00:00Z'),
        hostname: 'localhost',
        pid: 1234,
        source: 'test',
        level: 'info',
        message: 'test message',
        meta: 'meta data',
        errsole_id: 'err1'
      };
      errsolePostgres.pendingLogs = [sampleLog];

      fakeRelease = jest.fn();
      fakeClient = {
        query: jest.fn((query, queryParams, callback) => {
          // Simulate a query failure.
          callback(new Error('Query failed'));
        })
      };

      jest.spyOn(errsolePostgres.pool, 'connect').mockImplementation((callback) => {
        callback(null, fakeClient, fakeRelease);
      });

      await expect(errsolePostgres.flushLogs()).rejects.toThrow('Query failed');
      // Even if the query fails, the client should be released.
      expect(fakeRelease).toHaveBeenCalled();
    });
  });

  afterAll(() => {
    cronJob.stop();
    clearInterval(errsolePostgres.flushIntervalId);
  });
});
