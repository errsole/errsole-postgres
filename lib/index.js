const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const { EventEmitter } = require('events');

class ErrsolePostgres extends EventEmitter {
  constructor (options = {}) {
    super();
    if (!options.logging) options.logging = false;

    this.name = require('../package.json').name;
    this.version = require('../package.json').version || '0.0.0';
    this.isConnectionInProgress = true;

    this.pool = new Pool({
      host: options.host,
      user: options.username,
      password: options.password,
      database: options.database
    });

    this.initialize();
  }

  async initialize () {
    await this.checkConnection();
    await this.defineModels();
    this.ensureLogsTTL();
    this.setWorkMem();
    this.emit('ready');
  }

  async checkConnection () {
    return new Promise((resolve, reject) => {
      this.pool.connect((err, client, done) => {
        if (err) {
          console.error('Failed to get connection:', err);
          return reject(err);
        }
        done();
        resolve();
      });
    });
  }

  async defineModels () {
    const queries = [
      `CREATE TABLE IF NOT EXISTS errsole_logs (
        id BIGSERIAL PRIMARY KEY,
        hostname VARCHAR(255),
        pid INT,
        source VARCHAR(255),
        timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        level VARCHAR(255) DEFAULT 'info',
        message TEXT,
        meta TEXT
      )`,
      'CREATE INDEX IF NOT EXISTS errsole_logs_source_level_id_idx ON errsole_logs (source, level, id)',
      'CREATE INDEX IF NOT EXISTS errsole_logs_source_level_timestamp_idx ON errsole_logs (source, level, timestamp)',
      'CREATE INDEX IF NOT EXISTS errsole_logs_hostname_pid_id_idx ON errsole_logs (hostname, pid, id)',

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

    const query = `INSERT INTO errsole_logs (hostname, pid, source, level, message, meta) VALUES 
                   ${values.map((_, i) => `($${i * 6 + 1}, $${i * 6 + 2}, $${i * 6 + 3}, $${i * 6 + 4}, $${i * 6 + 5}, $${i * 6 + 6})`).join(', ')}`;

    return new Promise((resolve, reject) => {
      this.pool.query(query, values.flat(), (err) => {
        if (err) return reject(err);
        resolve();
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
    const query = `SELECT id, hostname, pid, source, timestamp, level, message FROM errsole_logs ${whereClause} ORDER BY id ${sortOrder} LIMIT $${values.length + 1}`;
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
    const query = `SELECT id, hostname, pid, source, timestamp, level, message FROM errsole_logs ${whereClause} ORDER BY id DESC LIMIT $${values.length + 1}`;
    values.push(filters.limit);

    return new Promise((resolve, reject) => {
      this.pool.query(query, values, (err, results) => {
        if (err) return reject(err);
        resolve({ items: results.rows });
      });
    });
  }

  async getMeta (id) {
    const query = 'SELECT id, meta FROM errsole_logs WHERE id = $1';
    return new Promise((resolve, reject) => {
      this.pool.query(query, [id], (err, results) => {
        if (err) return reject(err);
        if (!results.rows.length) return reject(new Error('Log entry not found.'));
        resolve({ item: results.rows[0] });
      });
    });
  }

  async ensureLogsTTL () {
    const DEFAULT_TTL = 2592000000;
    try {
      const configResult = await this.getConfig('logsTTL');
      if (!configResult.item) {
        await this.setConfig('logsTTL', DEFAULT_TTL.toString());
      }
      this.deleteExpiredLogs();
    } catch {}
  }

  async deleteExpiredLogs () {
    try {
      const configResult = await this.getConfig('logsTTL');
      if (!configResult || !configResult.item || !configResult.item.value) {
        throw new Error('Could not find the TTL configuration for logs.');
      }

      const logsTTL = parseInt(configResult.item.value, 10);

      while (true) {
        const expirationTime = new Date(Date.now() - logsTTL);
        // Format date for PostgreSQL
        const formattedExpirationTime = expirationTime.toISOString();

        const deleteQuery = `
          WITH rows AS (
            SELECT id
            FROM errsole_logs
            WHERE timestamp < $1
            LIMIT 1000
          )
          DELETE FROM errsole_logs
          WHERE id IN (SELECT id FROM rows);
        `;

        const results = await new Promise((resolve, reject) => {
          this.pool.query(deleteQuery, [formattedExpirationTime], (err, result) => {
            if (err) {
              reject(err);
            } else {
              resolve(result);
            }
          });
        });

        const deletedRowCount = results.rowCount;

        const delayTime = deletedRowCount > 0 ? 1000 : 3600000; // 1 second if rows were deleted, otherwise 1 hour
        await this.delay(delayTime);
      }
    } catch (err) {
      console.error(err);
    }
  }

  async delay (ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async createUser (user) {
    const SALT_ROUNDS = 10;
    const hashedPassword = await bcrypt.hash(user.password, SALT_ROUNDS);
    const query = 'INSERT INTO errsole_users (name, email, hashed_password, role) VALUES ($1, $2, $3, $4) RETURNING id';

    return new Promise((resolve, reject) => {
      this.pool.query(query, [user.name, user.email, hashedPassword, user.role], (err, results) => {
        if (err) {
          if (err.code === '23505') return reject(new Error('A user with the provided email already exists.'));
          return reject(err);
        }
        resolve({ item: { id: results.rows[0].id, name: user.name, email: user.email, role: user.role } });
      });
    });
  }

  async verifyUser (email, password) {
    if (!email || !password) throw new Error('Both email and password are required for verification.');

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
    if (!email || !currentPassword || !newPassword) throw new Error('Email, current password, and new password are required.');

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

  async deleteUser (userId) {
    if (!userId) throw new Error('User ID is required.');

    const query = 'DELETE FROM errsole_users WHERE id = $1';
    return new Promise((resolve, reject) => {
      this.pool.query(query, [userId], (err, results) => {
        if (err) return reject(err);
        if (results.rowCount === 0) return reject(new Error('User not found.'));
        resolve({});
      });
    });
  }
}

module.exports = ErrsolePostgres;
