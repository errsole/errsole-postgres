const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const cron = require('node-cron');
const { EventEmitter } = require('events');

class ErrsolePostgres extends EventEmitter {
  constructor (options = {}) {
    super();

    this.name = require('../package.json').name;
    this.version = require('../package.json').version || '0.0.0';

    this.isConnectionInProgress = true;
    this.pool = new Pool(options);
    this.initialize();
  }

  async initialize () {
    await this.checkConnection();
    await this.setWorkMem();
    await this.createTables();
    await this.ensureLogsTTL();
    this.emit('ready');
    cron.schedule('0 * * * *', () => {
      this.deleteExpiredLogs();
    });
  }

  async checkConnection () {
    return new Promise((resolve, reject) => {
      this.pool.connect((err, client, done) => {
        if (err) return reject(err);
        done();
        resolve();
      });
    });
  }

  async setWorkMem () {
    const desiredSize = 8192; // 8 MB in kilobytes
    const currentSize = await this.getWorkMem();

    if (currentSize < desiredSize) {
      const query = `SET work_mem = '${desiredSize}kB'`; // Set for the session
      return new Promise((resolve, reject) => {
        this.pool.query(query, (err) => {
          if (err) return reject(err);
          resolve();
        });
      });
    }
  }

  async getWorkMem () {
    const query = 'SHOW work_mem';
    return new Promise((resolve, reject) => {
      this.pool.query(query, (err, results) => {
        if (err) return reject(err);
        const currentSize = parseInt(results.rows[0].work_mem, 10);
        resolve(currentSize);
      });
    });
  }

  async createTables () {
    const queries = [
      `CREATE TABLE IF NOT EXISTS errsole_logs_v1 (
        id BIGSERIAL PRIMARY KEY,
        hostname VARCHAR(255),
        pid INT,
        source VARCHAR(255),
        timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        level VARCHAR(255) DEFAULT 'info',
        message TEXT,
        meta TEXT
      )`,
      'CREATE INDEX IF NOT EXISTS errsole_logs_v1_source_level_id_idx ON errsole_logs_v1 (source, level, id)',
      'CREATE INDEX IF NOT EXISTS errsole_logs_v1_source_level_timestamp_idx ON errsole_logs_v1 (source, level, timestamp)',
      'CREATE INDEX IF NOT EXISTS errsole_logs_v1_hostname_pid_id_idx ON errsole_logs_v1 (hostname, pid, id)',

      `CREATE TABLE IF NOT EXISTS errsole_users (
        id BIGSERIAL PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255) UNIQUE NOT NULL,
        hashed_password VARCHAR(255) NOT NULL,
        role VARCHAR(255) NOT NULL
      )`,

      `CREATE TABLE IF NOT EXISTS errsole_config (
        id BIGSERIAL PRIMARY KEY,
        key VARCHAR(255) UNIQUE NOT NULL,
        value VARCHAR(255) NOT NULL
      )`
    ];

    for (const query of queries) {
      await this.pool.query(query);
    }
    this.isConnectionInProgress = false;
  }

  async ensureLogsTTL () {
    const defaultTTL = 30 * 24 * 60 * 60 * 1000; // 30 days in milliseconds
    try {
      const configResult = await this.getConfig('logsTTL');
      if (!configResult.item) {
        await this.setConfig('logsTTL', defaultTTL.toString());
      }
    } catch (err) {
      console.error(err);
    }
    return {};
  }

  async getConfig (key) {
    const query = 'SELECT * FROM errsole_config WHERE key = $1';
    return new Promise((resolve, reject) => {
      this.pool.query(query, [key], (err, results) => {
        if (err) return reject(err);
        resolve({ item: results.rows[0] });
      });
    });
  }

  async setConfig (key, value) {
    const query = `INSERT INTO errsole_config (key, value) VALUES ($1, $2)
                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`;
    return new Promise((resolve, reject) => {
      this.pool.query(query, [key, value], (err) => {
        if (err) return reject(err);
        this.getConfig(key).then(resolve).catch(reject);
      });
    });
  }

  async deleteConfig (key) {
    const query = 'DELETE FROM errsole_config WHERE key = $1';
    return new Promise((resolve, reject) => {
      this.pool.query(query, [key], (err, results) => {
        if (err) return reject(err);
        if (results.rowCount === 0) return reject(new Error('Configuration not found.'));
        resolve({});
      });
    });
  }

  async postLogs (logEntries) {
    while (this.isConnectionInProgress) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    const values = logEntries.map(logEntry => [
      logEntry.hostname,
      logEntry.pid,
      logEntry.source,
      logEntry.level,
      logEntry.message,
      logEntry.meta
    ]);

    const query = 'INSERT INTO errsole_logs_v1 (hostname, pid, source, level, message, meta) VALUES ($1, $2, $3, $4, $5, $6)';

    return new Promise((resolve, reject) => {
      this.pool.query(query, values.flat(), (err) => {
        if (err) return reject(err);
        resolve({});
      });
    });
  }

  async getLogs (filters = {}) {
    const whereClauses = [];
    const values = [];
    const defaultLimit = 100;
    filters.limit = filters.limit || defaultLimit;
    let sortOrder = 'DESC';
    let shouldReverse = true;

    // Apply filters
    if (filters.hostname) {
      whereClauses.push('hostname = $' + (values.length + 1));
      values.push(filters.hostname);
    }
    if (filters.pid) {
      whereClauses.push('pid = $' + (values.length + 1));
      values.push(filters.pid);
    }
    if (filters.sources && filters.sources.length > 0) {
      whereClauses.push('source = ANY($' + (values.length + 1) + ')');
      values.push(filters.sources);
    }
    if (filters.levels && filters.levels.length > 0) {
      whereClauses.push('level = ANY($' + (values.length + 1) + ')');
      values.push(filters.levels);
    }
    if (filters.level_json && filters.level_json.length > 0) {
      const levelConditions = filters.level_json.map((levelObj, i) => {
        values.push(levelObj.source, levelObj.level);
        return `(source = $${values.length - 1} AND level = $${values.length})`;
      });
      whereClauses.push(`(${levelConditions.join(' OR ')})`);
    }
    if (filters.lt_id) {
      whereClauses.push('id < $' + (values.length + 1));
      values.push(filters.lt_id);
      sortOrder = 'DESC';
      shouldReverse = true;
    } else if (filters.gt_id) {
      whereClauses.push('id > $' + (values.length + 1));
      values.push(filters.gt_id);
      sortOrder = 'ASC';
      shouldReverse = false;
    } else if (filters.lte_timestamp || filters.gte_timestamp) {
      if (filters.lte_timestamp) {
        whereClauses.push('timestamp <= $' + (values.length + 1));
        values.push(new Date(filters.lte_timestamp));
        sortOrder = 'DESC';
        shouldReverse = true;
      }
      if (filters.gte_timestamp) {
        whereClauses.push('timestamp >= $' + (values.length + 1));
        values.push(new Date(filters.gte_timestamp));
        sortOrder = 'ASC';
        shouldReverse = false;
      }
    }

    const whereClause = whereClauses.length ? `WHERE ${whereClauses.join(' AND ')}` : '';
    const query = `SELECT id, hostname, pid, source, timestamp, level, message FROM errsole_logs_v1 ${whereClause} ORDER BY id ${sortOrder} LIMIT $${values.length + 1}`;
    values.push(filters.limit);

    return new Promise((resolve, reject) => {
      this.pool.query(query, values, (err, results) => {
        if (err) return reject(err);
        if (shouldReverse) results.rows.reverse();
        resolve({ items: results.rows });
      });
    });
  }

  async searchLogs (searchTerms, filters = {}) {
    const whereClauses = searchTerms.map((_, index) => `message ILIKE $${index + 1}`);
    const values = searchTerms.map(term => `%${term}%`);
    filters.limit = filters.limit || 100;

    if (filters.hostname) {
      whereClauses.push(`hostname = $${values.length + 1}`);
      values.push(filters.hostname);
    }
    if (filters.pid) {
      whereClauses.push(`pid = $${values.length + 1}`);
      values.push(filters.pid);
    }
    if (filters.sources) {
      whereClauses.push(`source = ANY($${values.length + 1})`);
      values.push(filters.sources);
    }
    if (filters.levels) {
      whereClauses.push(`level = ANY($${values.length + 1})`);
      values.push(filters.levels);
    }
    if (filters.level_json) {
      filters.level_json.forEach(levelObj => {
        whereClauses.push(`(source = $${values.length + 1} AND level = $${values.length + 2})`);
        values.push(levelObj.source, levelObj.level);
      });
    }
    if (filters.lt_id) {
      whereClauses.push(`id < $${values.length + 1}`);
      values.push(filters.lt_id);
    } else if (filters.gt_id) {
      whereClauses.push(`id > $${values.length + 1}`);
      values.push(filters.gt_id);
    } else if (filters.lte_timestamp || filters.gte_timestamp) {
      if (filters.lte_timestamp) {
        whereClauses.push(`timestamp <= $${values.length + 1}`);
        values.push(new Date(filters.lte_timestamp));
      }
      if (filters.gte_timestamp) {
        whereClauses.push(`timestamp >= $${values.length + 1}`);
        values.push(new Date(filters.gte_timestamp));
      }
    }

    const whereClause = whereClauses.length ? `WHERE ${whereClauses.join(' AND ')}` : '';
    const query = `SELECT id, hostname, pid, source, timestamp, level, message FROM errsole_logs_v1 ${whereClause} ORDER BY id DESC LIMIT $${values.length + 1}`;
    values.push(filters.limit);

    return new Promise((resolve, reject) => {
      this.pool.query(query, values, (err, results) => {
        if (err) return reject(err);
        resolve({ items: results.rows });
      });
    });
  }

  async getMeta (id) {
    const query = 'SELECT id, meta FROM errsole_logs_v1 WHERE id = $1';
    return new Promise((resolve, reject) => {
      this.pool.query(query, [id], (err, results) => {
        if (err) return reject(err);
        if (!results.rows.length) return reject(new Error('Log entry not found.'));
        resolve({ item: results.rows[0] });
      });
    });
  }

  async deleteExpiredLogs () {
    if (this.deleteExpiredLogsRunning) return;

    this.deleteExpiredLogsRunning = true;

    const defaultLogsTTL = 30 * 24 * 60 * 60 * 1000; // 30 days in milliseconds

    try {
      let logsTTL = defaultLogsTTL;
      const configResult = await this.getConfig('logsTTL');
      if (configResult.item) {
        const parsedTTL = parseInt(configResult.item.value, 10);
        logsTTL = isNaN(parsedTTL) ? defaultLogsTTL : parsedTTL;
      }
      let expirationTime = new Date(Date.now() - logsTTL);
      expirationTime = new Date(expirationTime).toISOString();
      let deletedRowCount;
      do {
        deletedRowCount = await new Promise((resolve, reject) => {
          this.pool.query(
            `WITH deleted AS (
             SELECT id FROM errsole_logs_v1
             WHERE timestamp < $1 
             LIMIT 1000
            )
            DELETE FROM errsole_logs_v1 
            WHERE id IN (SELECT id FROM deleted)
            RETURNING *`,
            [expirationTime],
            (err, result) => {
              if (err) return reject(err);
              if (!result) return reject(new Error('Unexpected result format'));
              resolve(result.rowCount);
            }
          );
        });
        await new Promise(resolve => setTimeout(resolve, 10000));
      } while (deletedRowCount > 0);
    } catch (err) {
      console.error(err);
    } finally {
      this.deleteExpiredLogsRunning = false;
    }
  }

  async createUser (user) {
    const SALT_ROUNDS = 10;
    const hashedPassword = await bcrypt.hash(user.password, SALT_ROUNDS);
    const query = 'INSERT INTO errsole_users (name, email, hashed_password, role) VALUES ($1, $2, $3, $4) RETURNING id';

    return new Promise((resolve, reject) => {
      this.pool.query(query, [user.name, user.email, hashedPassword, user.role], (err, results) => {
        if (err) {
          if (err.code === '23505') {
            return reject(new Error('A user with the provided email already exists.'));
          }
          return reject(err);
        }
        resolve({ item: { id: results.rows[0].id, name: user.name, email: user.email, role: user.role } });
      });
    });
  }

  async verifyUser (email, password) {
    if (!email || !password) {
      throw new Error('Both email and password are required for verification.');
    }

    const query = 'SELECT * FROM errsole_users WHERE email = $1';
    return new Promise((resolve, reject) => {
      this.pool.query(query, [email], async (err, results) => {
        if (err) return reject(err);
        if (!results.rows.length) return reject(new Error('User not found.'));

        const user = results.rows[0];
        const isPasswordCorrect = await bcrypt.compare(password, user.hashed_password);
        if (!isPasswordCorrect) return reject(new Error('Incorrect password.'));

        delete user.hashed_password;
        resolve({ item: user });
      });
    });
  }

  async getUserCount () {
    const query = 'SELECT COUNT(*) as count FROM errsole_users';
    return new Promise((resolve, reject) => {
      this.pool.query(query, (err, results) => {
        if (err) return reject(err);
        resolve({ count: parseInt(results.rows[0].count, 10) });
      });
    });
  }

  async getAllUsers () {
    const query = 'SELECT id, name, email, role FROM errsole_users';
    return new Promise((resolve, reject) => {
      this.pool.query(query, (err, results) => {
        if (err) return reject(err);
        resolve({ items: results.rows });
      });
    });
  }

  async getUserByEmail (email) {
    if (!email) throw new Error('Email is required.');

    const query = 'SELECT id, name, email, role FROM errsole_users WHERE email = $1';
    return new Promise((resolve, reject) => {
      this.pool.query(query, [email], (err, results) => {
        if (err) return reject(err);
        if (!results.rows.length) return reject(new Error('User not found.'));
        resolve({ item: results.rows[0] });
      });
    });
  }

  async updateUserByEmail (email, updates) {
    if (!email) throw new Error('Email is required.');
    if (!updates || Object.keys(updates).length === 0) throw new Error('No updates provided.');

    const restrictedFields = ['id', 'hashed_password'];
    restrictedFields.forEach(field => delete updates[field]);

    const setClause = Object.keys(updates).map((key, i) => `${key} = $${i + 1}`).join(', ');
    const values = [...Object.values(updates), email];

    const query = `UPDATE errsole_users SET ${setClause} WHERE email = $${values.length}`;
    return new Promise((resolve, reject) => {
      this.pool.query(query, values, async (err, results) => {
        if (err) return reject(err);
        if (results.rowCount === 0) return reject(new Error('No updates applied.'));
        this.getUserByEmail(email).then(resolve).catch(reject);
      });
    });
  }

  async updatePassword (email, currentPassword, newPassword) {
    if (!email || !currentPassword || !newPassword) {
      throw new Error('Email, current password, and new password are required.');
    }

    const query = 'SELECT * FROM errsole_users WHERE email = $1';
    return new Promise((resolve, reject) => {
      this.pool.query(query, [email], async (err, results) => {
        if (err) return reject(err);
        if (!results.rows.length) return reject(new Error('User not found.'));

        const user = results.rows[0];
        const isPasswordCorrect = await bcrypt.compare(currentPassword, user.hashed_password);
        if (!isPasswordCorrect) return reject(new Error('Current password is incorrect.'));

        const hashedPassword = await bcrypt.hash(newPassword, 10);
        const updateQuery = 'UPDATE errsole_users SET hashed_password = $1 WHERE email = $2';
        this.pool.query(updateQuery, [hashedPassword, email], (err, updateResults) => {
          if (err) return reject(err);
          if (updateResults.rowCount === 0) return reject(new Error('Password update failed.'));
          delete user.hashed_password;
          resolve({ item: user });
        });
      });
    });
  }

  /**
   * Deletes a user record from the database.
   *
   * @async
   * @function deleteUser
   * @param {number} id - The unique ID of the user to be deleted.
   * @returns {Promise<{}>} - A Promise that resolves with an empty object upon successful deletion of the user.
   * @throws {Error} - Throws an error if no user is found with the given ID or if the database operation fails.
   */
  async deleteUser (id) {
    if (!id) throw new Error('User ID is required.');

    const query = 'DELETE FROM errsole_users WHERE id = $1';
    return new Promise((resolve, reject) => {
      this.pool.query(query, [id], (err, results) => {
        if (err) return reject(err);
        if (results.rowCount === 0) return reject(new Error('User not found.'));
        resolve({});
      });
    });
  }
}

module.exports = ErrsolePostgres;
