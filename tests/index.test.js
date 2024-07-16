const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const cron = require('node-cron');
const ErrsolePostgres = require('../lib/index'); // Adjust the path as needed
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
      query: jest.fn().mockResolvedValue({ rows: [{ work_mem: '8192kB' }] })
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

  describe('#checkConnection', () => {
    it('should check the database connection', async () => {
      const client = { query: jest.fn().mockResolvedValue({}), release: jest.fn() };
      poolMock.connect.mockResolvedValue(client);

      await errsolePostgres.checkConnection();

      expect(poolMock.connect).toHaveBeenCalled();
      expect(client.query).toHaveBeenCalledWith('SELECT NOW()');
      expect(client.release).toHaveBeenCalled();
    });
  });

  describe('#setWorkMem', () => {
    it('should set work_mem if current size is less than desired', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [{ work_mem: '4096kB' }] });

      await errsolePostgres.setWorkMem();

      expect(poolMock.query).toHaveBeenCalledWith('SHOW work_mem');
      expect(poolMock.query).toHaveBeenCalledWith("SET work_mem = '8192kB'");
    });
  });

  describe('#createTables', () => {
    it('should create necessary tables', async () => {
      poolMock.query.mockResolvedValue({});

      await errsolePostgres.createTables();

      const createTableCalls = [
        expect.stringContaining('CREATE TABLE IF NOT EXISTS errsole_logs_v1'),
        expect.stringContaining('CREATE INDEX IF NOT EXISTS errsole_logs_v1_source_level_id_idx'),
        expect.stringContaining('CREATE INDEX IF NOT EXISTS errsole_logs_v1_source_level_timestamp_idx'),
        expect.stringContaining('CREATE INDEX IF NOT EXISTS errsole_logs_v1_hostname_pid_id_idx'),
        expect.stringContaining('CREATE TABLE IF NOT EXISTS errsole_users'),
        expect.stringContaining('CREATE TABLE IF NOT EXISTS errsole_config')
      ];

      createTableCalls.forEach(call => {
        expect(poolMock.query).toHaveBeenCalledWith(call);
      });
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

    it('should return undefined if configuration key is not found', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [] });

      const result = await errsolePostgres.getConfig('nonexistentKey');

      expect(poolMock.query).toHaveBeenCalledWith('SELECT * FROM errsole_config WHERE key = $1', ['nonexistentKey']);
      expect(result).toEqual({ item: undefined });
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

  describe('#createUser', () => {
    it('should create a new user', async () => {
      const user = { name: 'John', email: 'john@example.com', password: 'password', role: 'admin' };
      const hashedPassword = await bcrypt.hash(user.password, 10);
      poolMock.query.mockResolvedValueOnce({ rows: [{ id: 1 }] });

      const result = await errsolePostgres.createUser(user);

      expect(poolMock.query).toHaveBeenCalledWith(
        'INSERT INTO errsole_users (name, email, hashed_password, role) VALUES ($1, $2, $3, $4) RETURNING id',
        [user.name, user.email, hashedPassword, user.role]
      );
      expect(result).toEqual({ item: { id: 1, name: user.name, email: user.email, role: user.role } });
    });
  });

  describe('#verifyUser', () => {
    it('should verify user credentials', async () => {
      const user = { email: 'john@example.com', hashed_password: await bcrypt.hash('password', 10), role: 'admin' };
      poolMock.query.mockResolvedValueOnce({ rows: [user] });
      bcrypt.compare.mockResolvedValueOnce(true); // Simulate correct password

      const result = await errsolePostgres.verifyUser(user.email, 'password');

      expect(result).toEqual({ item: { id: user.id, name: user.name, email: user.email, role: user.role } });
    });

    it('should throw error if user not found', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [] });

      await expect(errsolePostgres.verifyUser('nonexistent@example.com', 'password')).rejects.toThrow('User not found.');
    });

    it('should throw error if password is incorrect', async () => {
      const user = { email: 'john@example.com', hashed_password: await bcrypt.hash('password', 10), role: 'admin' };
      poolMock.query.mockResolvedValueOnce({ rows: [user] });
      bcrypt.compare.mockResolvedValueOnce(false); // Simulate incorrect password

      await expect(errsolePostgres.verifyUser(user.email, 'wrongpassword')).rejects.toThrow('Incorrect password.');
    });
  });

  describe('#getAllUsers', () => {
    it('should retrieve all users', async () => {
      const users = [{ id: 1, name: 'John', email: 'john@example.com', role: 'admin' }];
      poolMock.query.mockResolvedValueOnce({ rows: users });

      const result = await errsolePostgres.getAllUsers();

      expect(poolMock.query).toHaveBeenCalledWith('SELECT id, name, email, role FROM errsole_users');
      expect(result).toEqual({ items: users });
    });
  });

  describe('#getUserByEmail', () => {
    it('should retrieve user by email', async () => {
      const user = { id: 1, name: 'John', email: 'john@example.com', role: 'admin' };
      poolMock.query.mockResolvedValueOnce({ rows: [user] });

      const result = await errsolePostgres.getUserByEmail('john@example.com');

      expect(poolMock.query).toHaveBeenCalledWith('SELECT id, name, email, role FROM errsole_users WHERE email = $1', ['john@example.com']);
      expect(result).toEqual({ item: user });
    });

    it('should throw error if user not found', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [] });

      await expect(errsolePostgres.getUserByEmail('nonexistent@example.com')).rejects.toThrow('User not found.');
    });
  });

  describe('#updateUserByEmail', () => {
    it('should update user by email', async () => {
      const updates = { name: 'John Updated' };
      poolMock.query.mockResolvedValueOnce({ rowCount: 1 });
      const user = { id: 1, name: 'John Updated', email: 'john@example.com', role: 'admin' };
      poolMock.query.mockResolvedValueOnce({ rows: [user] });

      const result = await errsolePostgres.updateUserByEmail('john@example.com', updates);

      expect(poolMock.query).toHaveBeenCalledWith(
        'UPDATE errsole_users SET name = $1 WHERE email = $2',
        ['John Updated', 'john@example.com']
      );
      expect(result).toEqual({ item: user });
    });

    it('should throw an error if no email is provided', async () => {
      await expect(errsolePostgres.updateUserByEmail('', { name: 'updated' })).rejects.toThrow('Email is required.');
    });

    it('should throw an error if no updates are provided', async () => {
      await expect(errsolePostgres.updateUserByEmail('john@example.com', {})).rejects.toThrow('No updates provided.');
    });

    it('should throw an error if user not found', async () => {
      poolMock.query.mockResolvedValueOnce({ rowCount: 0 });

      await expect(errsolePostgres.updateUserByEmail('nonexistent@example.com', { name: 'updated' })).rejects.toThrow('No updates applied.');
    });
  });

  describe('#updatePassword', () => {
    it('should update user password', async () => {
      const user = { email: 'john@example.com', hashed_password: await bcrypt.hash('password', 10), role: 'admin' };
      poolMock.query.mockResolvedValueOnce({ rows: [user] });
      bcrypt.compare.mockResolvedValueOnce(true); // Simulate correct current password
      const newHashedPassword = await bcrypt.hash('newpassword', 10);
      bcrypt.hash.mockResolvedValueOnce(newHashedPassword);
      poolMock.query.mockResolvedValueOnce({ rowCount: 1 });

      const result = await errsolePostgres.updatePassword('john@example.com', 'password', 'newpassword');

      expect(poolMock.query).toHaveBeenCalledWith('SELECT * FROM errsole_users WHERE email = $1', ['john@example.com']);
      expect(bcrypt.compare).toHaveBeenCalledWith('password', user.hashed_password);
      expect(bcrypt.hash).toHaveBeenCalledWith('newpassword', 10);
      expect(poolMock.query).toHaveBeenCalledWith('UPDATE errsole_users SET hashed_password = $1 WHERE email = $2', [newHashedPassword, 'john@example.com']);
      expect(result).toEqual({ item: { id: user.id, name: user.name, email: user.email, role: user.role } });
    });

    it('should throw an error if email, current password, or new password is missing', async () => {
      await expect(errsolePostgres.updatePassword('', 'password', 'newpassword')).rejects.toThrow('Email, current password, and new password are required.');
      await expect(errsolePostgres.updatePassword('john@example.com', '', 'newpassword')).rejects.toThrow('Email, current password, and new password are required.');
      await expect(errsolePostgres.updatePassword('john@example.com', 'password', '')).rejects.toThrow('Email, current password, and new password are required.');
    });

    it('should throw an error if user is not found', async () => {
      poolMock.query.mockResolvedValueOnce({ rows: [] });

      await expect(errsolePostgres.updatePassword('nonexistent@example.com', 'password', 'newpassword')).rejects.toThrow('User not found.');
    });

    it('should throw an error if current password is incorrect', async () => {
      const user = { email: 'john@example.com', hashed_password: await bcrypt.hash('password', 10) };
      poolMock.query.mockResolvedValueOnce({ rows: [user] });
      bcrypt.compare.mockResolvedValueOnce(false); // Simulate incorrect current password

      await expect(errsolePostgres.updatePassword('john@example.com', 'wrongpassword', 'newpassword')).rejects.toThrow('Current password is incorrect.');
    });
  });

  describe('#deleteUser', () => {
    it('should delete user by id', async () => {
      poolMock.query.mockResolvedValueOnce({ rowCount: 1 });

      await errsolePostgres.deleteUser(1);

      expect(poolMock.query).toHaveBeenCalledWith('DELETE FROM errsole_users WHERE id = $1', [1]);
    });

    it('should throw error if user not found', async () => {
      poolMock.query.mockResolvedValueOnce({ rowCount: 0 });

      await expect(errsolePostgres.deleteUser(1)).rejects.toThrow('User not found.');
    });
  });

  // Additional tests go here

  afterAll(() => {
    // Stop the cron job
    cronJob.stop();
    // Clear the interval
    clearInterval(errsolePostgres.flushIntervalId);
  });
});
