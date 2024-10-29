/**
 * @typedef {Object} Log
 * @property {number} [id]
 * @property {number} [errsole_id]
 * @property {Date} timestamp
 * @property {string} hostname
 * @property {string} source
 * @property {string} level
 * @property {string} message
 * @property {string} [meta]
 */

/**
 * @typedef {Object} LogFilter
 * @property {number} [lt_id]
 * @property {number} [gt_id]
 * @property {number} [errsole_id]
 * @property {Date} [lte_timestamp]
 * @property {Date} [gte_timestamp]
 * @property {string[]} [hostnames]
 * @property {{source: string, level: string}[]} [level_json]
 * @property {number} [limit=100]
 */

/**
 * @typedef {Object} Config
 * @property {number} id
 * @property {string} key
 * @property {string} value
 */

/**
 * @typedef {Object} User
 * @property {number} id
 * @property {string} name
 * @property {string} email
 * @property {string} role
 */

/**
 * @typedef {Object} Notification
 * @property {number} [id]
 * @property {number} [errsole_id]
 * @property {string} hostname
 * @property {string} hashed_message
 * @property {Date} [created_at]
 * @property {Date} [updated_at]
 */

const bcrypt = require('bcryptjs');
const { EventEmitter } = require('events');
const cron = require('node-cron');
const { Pool } = require('pg');

class ErrsolePostgres extends EventEmitter {
  constructor (options = {}) {
    super();

    this.name = require('../package.json').name;
    this.version = require('../package.json').version || '0.0.0';

    this.isConnectionInProgress = true;
    this.pool = new Pool(options);

    this.pendingLogs = [];
    this.batchSize = 100;
    this.flushInterval = 1000;

    this.initialize();
  }

  async initialize () {
    await this.checkConnection();
    await this.setWorkMem();
    await this.createTables();
    await this.ensureLogsTTL();
    this.emit('ready');
    setInterval(() => this.flushLogs(), this.flushInterval);
    cron.schedule('0 * * * *', () => {
      this.deleteExpiredLogs();
      this.deleteExpiredNotificationItems();
    });
  }

  async checkConnection () {
    const client = await this.pool.connect();
    await client.query('SELECT NOW()');
    client.release();
  }

  async setWorkMem () {
    const DESIRED_WORK_MEM = 8192; // 8 MB in kilobytes
    const currentSize = await this.getWorkMem();
    if (currentSize < DESIRED_WORK_MEM) {
      const query = `SET work_mem = '${DESIRED_WORK_MEM}kB'`; // Set for the session
      await this.pool.query(query);
    }
  }

  async getWorkMem () {
    const query = 'SHOW work_mem';
    const { rows } = await this.pool.query(query);
    return parseInt(rows[0].work_mem, 10);
  }

  async createTables () {
    const queries = [
      `CREATE TABLE IF NOT EXISTS errsole_logs_v2 (
        id BIGSERIAL PRIMARY KEY,
        hostname VARCHAR(255),
        pid INT,
        source VARCHAR(255),
        timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        level VARCHAR(255) DEFAULT 'info',
        message TEXT,
        meta TEXT,
        errsole_id BIGINT
      )`,
      'CREATE INDEX IF NOT EXISTS errsole_logs_v2_source_level_id_idx ON errsole_logs_v2 (source, level, id)',
      'CREATE INDEX IF NOT EXISTS errsole_logs_v2_source_level_timestamp_idx ON errsole_logs_v2 (source, level, timestamp)',
      'CREATE INDEX IF NOT EXISTS errsole_logs_v2_hostname_pid_id_idx ON errsole_logs_v2 (hostname, pid, id)',
      'CREATE INDEX IF NOT EXISTS idx_errsole_id ON errsole_logs_v2 (errsole_id);',

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
      )`,

      `CREATE TABLE IF NOT EXISTS errsole_notifications (
        id BIGSERIAL PRIMARY KEY,
        errsole_id BIGINT,
        hostname VARCHAR(255),
        hashed_message VARCHAR(255),
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
      )`,
      'CREATE INDEX IF NOT EXISTS idx_errsole_notifications_host_hashed_created ON errsole_notifications (hostname, hashed_message, created_at);',
      'CREATE INDEX IF NOT EXISTS idx_errsole_notifications_created_at ON errsole_notifications (created_at);'

    ];

    for (const query of queries) {
      await this.pool.query(query);
    }
    this.isConnectionInProgress = false;
  }

  async ensureLogsTTL () {
    const DEFAULT_LOGS_TTL = 30 * 24 * 60 * 60 * 1000; // 30 days in milliseconds
    const configResult = await this.getConfig('logsTTL');
    if (!configResult.item) {
      await this.setConfig('logsTTL', DEFAULT_LOGS_TTL.toString());
    }
  }

  /**
   * Retrieves a configuration entry from the database.
   *
   * @async
   * @function getConfig
   * @param {string} key - The key of the configuration entry to retrieve.
   * @returns {Promise<{item: Config}>} - A promise that resolves with an object containing the configuration item.
   * @throws {Error} - Throws an error if the operation fails.
   */
  async getConfig (key) {
    const query = 'SELECT * FROM errsole_config WHERE key = $1';
    const { rows } = await this.pool.query(query, [key]);
    return { item: rows[0] };
  }

  /**
   * Updates or adds a configuration entry in the database.
   *
   * @async
   * @function setConfig
   * @param {string} key - The key of the configuration entry.
   * @param {string} value - The value to be stored for the configuration entry.
   * @returns {Promise<{item: Config}>} - A promise that resolves with an object containing the updated or added configuration item.
   * @throws {Error} - Throws an error if the operation fails.
   */
  async setConfig (key, value) {
    const query = `INSERT INTO errsole_config (key, value) VALUES ($1, $2)
                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`;
    await this.pool.query(query, [key, value]);
    return await this.getConfig(key);
  }

  /**
   * Deletes a configuration entry from the database.
   *
   * @async
   * @function deleteConfig
   * @param {string} key - The key of the configuration entry to be deleted.
   * @returns {Promise<{}>} - A Promise that resolves with an empty object upon successful deletion of the configuration.
   * @throws {Error} - Throws an error if the operation fails.
   */
  async deleteConfig (key) {
    const query = 'DELETE FROM errsole_config WHERE key = $1';
    await this.pool.query(query, [key]);
    return {};
  }

  /**
   * Adds log entries to the pending logs and flushes them if the batch size is reached.
   *
   * @param {Log[]} logEntries - An array of log entries to be added to the pending logs.
   * @returns {Object} - An empty object.
   */
  postLogs (logEntries) {
    this.pendingLogs.push(...logEntries);
    if (this.pendingLogs.length >= this.batchSize) {
      this.flushLogs();
    }
    return {};
  }

  /**
   * Flushes pending logs to the database.
   *
   * @async
   * @function flushLogs
   * @returns {Promise<{}>} - A Promise that resolves with an empty object upon successful flush.
   * @throws {Error} - Throws an error if the operation fails.
   */
  async flushLogs () {
    while (this.isConnectionInProgress) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    const logsToPost = this.pendingLogs.splice(0, this.pendingLogs.length);
    if (logsToPost.length === 0) {
      return {}; // No logs to post
    }

    const values = logsToPost.map(logEntry => [
      new Date(logEntry.timestamp),
      logEntry.hostname,
      logEntry.pid,
      logEntry.source,
      logEntry.level,
      logEntry.message,
      logEntry.meta,
      logEntry.errsole_id
    ]);
    const query = `
    INSERT INTO errsole_logs_v2 (timestamp, hostname, pid, source, level, message, meta, errsole_id)
    VALUES ${logsToPost.map((_, i) =>
      `($${i * 8 + 1}, $${i * 8 + 2}, $${i * 8 + 3}, $${i * 8 + 4}, $${i * 8 + 5}, $${i * 8 + 6}, $${i * 8 + 7}, $${i * 8 + 8})`
    ).join(', ')}
  `;

    const queryParams = values.flat();
    return await new Promise((resolve, reject) => {
      this.pool.connect((err, client, release) => {
        if (err) return reject(err);
        client.query(query, queryParams, err => {
          release();
          if (err) return reject(err);
          resolve({});
        });
      });
    });
  }

  /**
   * Retrieves unique hostnames from the database.
   *
   * @async
   * @function getHostnames
   * @returns {Promise<{items: string[]}>} - A Promise that resolves with an object containing an array of unique hostnames.
   * @throws {Error} - Throws an error if the operation fails.
   */
  async getHostnames () {
    const query = `
      SELECT DISTINCT hostname 
      FROM errsole_logs_v2 
      WHERE hostname IS NOT NULL AND hostname != ''
    `;

    return new Promise((resolve, reject) => {
      this.pool.query(query, (err, result) => {
        if (err) return reject(err);
        const hostnames = result.rows.map(row => row.hostname).sort();
        resolve({ items: hostnames });
      });
    });
  }

  /**
   * Retrieves log entries from the database based on specified filters.
   *
   * @async
   * @function getLogs
   * @param {LogFilter} [filters] - Filters to apply for log retrieval.
   * @returns {Promise<{items: Log[]}>} - A Promise that resolves with an object containing log items.
   * @throws {Error} - Throws an error if the operation fails.
   */
  async getLogs (filters = {}) {
    const DEFAULT_LOGS_LIMIT = 100;

    filters.limit = filters.limit || DEFAULT_LOGS_LIMIT;

    const whereClauses = [];
    const values = [];
    let sortOrder = 'DESC';
    let shouldReverse = true;

    // Apply filters
    if (filters.hostname) {
      whereClauses.push('hostname = $' + (values.length + 1));
      values.push(filters.hostname);
    }
    if (filters.hostnames && filters.hostnames.length > 0) {
      whereClauses.push('hostname = ANY($' + (values.length + 1) + ')');
      values.push(filters.hostnames);
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
    if (filters.level_json || filters.errsole_id) {
      const orConditions = [];

      // Handling level_json filter
      if (filters.level_json && filters.level_json.length > 0) {
        const levelConditions = filters.level_json.map((levelObj, i) => {
          values.push(levelObj.source, levelObj.level);
          return `(source = $${values.length - 1} AND level = $${values.length})`;
        });
        orConditions.push(`(${levelConditions.join(' OR ')})`);
      }

      // Handling errsole_id filter
      if (filters.errsole_id) {
        orConditions.push(`errsole_id = $${values.length + 1}`);
        values.push(filters.errsole_id);
      }

      // Add the OR conditions to the WHERE clause
      whereClauses.push(`(${orConditions.join(' OR ')})`);
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
    const query = `SELECT id, hostname, pid, source, timestamp, level, message, errsole_id FROM errsole_logs_v2 ${whereClause} ORDER BY id ${sortOrder} LIMIT $${values.length + 1}`;
    values.push(filters.limit);

    const { rows } = await this.pool.query(query, values);
    if (shouldReverse) rows.reverse();
    return { items: rows };
  }

  /**
   * Retrieves log entries from the database based on specified search terms and filters.
   *
   * @async
   * @function searchLogs
   * @param {string[]} searchTerms - An array of search terms.
   * @param {LogFilter} [filters] - Filters to refine the search.
   * @returns {Promise<{items: Log[], filters: LogFilter[]}>} - A promise that resolves with an object containing an array of log items and the applied filters.
   * @throws {Error} - Throws an error if the operation fails.
   */
  async searchLogs (searchTerms, filters = {}) {
    const DEFAULT_LOGS_LIMIT = 100;
    filters.limit = filters.limit || DEFAULT_LOGS_LIMIT;

    const whereClauses = [];
    const values = [];
    let sortOrder = 'DESC';
    let shouldReverse = true;
    let index = 1;

    // Search terms with AND logic
    if (searchTerms && searchTerms.length > 0) {
      searchTerms.forEach(term => {
        whereClauses.push(`message ILIKE $${index++}`);
        values.push(`%${term}%`);
      });
    }

    // Apply filters
    if (filters.hostname) {
      whereClauses.push(`hostname = $${index++}`);
      values.push(filters.hostname);
    }
    if (filters.hostnames && filters.hostnames.length > 0) {
      whereClauses.push('hostname = ANY($' + (values.length + 1) + ')');
      values.push(filters.hostnames);
    }
    if (filters.pid) {
      whereClauses.push(`pid = $${index++}`);
      values.push(filters.pid);
    }
    if (filters.sources && filters.sources.length > 0) {
      whereClauses.push(`source = ANY($${index++})`);
      values.push(filters.sources);
    }
    if (filters.levels && filters.levels.length > 0) {
      whereClauses.push(`level = ANY($${index++})`);
      values.push(filters.levels);
    }
    if (filters.level_json && filters.level_json.length > 0) {
      const levelConditions = filters.level_json.map(levelObj => `(source = $${index++} AND level = $${index++})`);
      whereClauses.push(`(${levelConditions.join(' OR ')})`);
      filters.level_json.forEach(levelObj => {
        values.push(levelObj.source, levelObj.level);
      });
    }
    if (filters.level_json || filters.errsole_id) {
      const orConditions = [];

      // Handling level_json filter
      if (filters.level_json && filters.level_json.length > 0) {
        const levelConditions = filters.level_json.map((levelObj, i) => {
          values.push(levelObj.source, levelObj.level);
          return `(source = $${values.length - 1} AND level = $${values.length})`;
        });
        orConditions.push(`(${levelConditions.join(' OR ')})`);
      }

      // Handling errsole_id filter
      if (filters.errsole_id) {
        orConditions.push(`errsole_id = $${values.length + 1}`);
        values.push(filters.errsole_id);
      }

      // Add the OR conditions to the WHERE clause
      whereClauses.push(`(${orConditions.join(' OR ')})`);
    }
    if (filters.lt_id) {
      whereClauses.push(`id < $${index++}`);
      values.push(filters.lt_id);
      sortOrder = 'DESC';
      shouldReverse = true;
    }
    if (filters.gt_id) {
      whereClauses.push(`id > $${index++}`);
      values.push(filters.gt_id);
      sortOrder = 'ASC';
      shouldReverse = false;
    }
    if (filters.lte_timestamp || filters.gte_timestamp) {
      if (filters.lte_timestamp) {
        filters.lte_timestamp = new Date(filters.lte_timestamp);
        whereClauses.push(`timestamp <= $${index++}`);
        values.push(filters.lte_timestamp);
        sortOrder = 'DESC';
        shouldReverse = true;
      }
      if (filters.gte_timestamp) {
        filters.gte_timestamp = new Date(filters.gte_timestamp);
        whereClauses.push(`timestamp >= $${index++}`);
        values.push(filters.gte_timestamp);
        sortOrder = 'ASC';
        shouldReverse = false;
      }
      if (filters.lte_timestamp && !filters.gte_timestamp) {
        const gteTimestamp = new Date(filters.lte_timestamp.getTime() - 24 * 60 * 60 * 1000);
        whereClauses.push(`timestamp >= $${index++}`);
        values.push(gteTimestamp);
        filters.gte_timestamp = gteTimestamp;
      }
      if (filters.gte_timestamp && !filters.lte_timestamp) {
        const lteTimestamp = new Date(filters.gte_timestamp.getTime() + 24 * 60 * 60 * 1000);
        whereClauses.push(`timestamp <= $${index++}`);
        values.push(lteTimestamp);
        filters.lte_timestamp = lteTimestamp;
      }
    }

    const whereClause = whereClauses.length ? `WHERE ${whereClauses.join(' AND ')}` : '';
    const query = `SELECT id, hostname, pid, source, timestamp, level, message,errsole_id FROM errsole_logs_v2 ${whereClause} ORDER BY id ${sortOrder} LIMIT $${index}`;
    values.push(filters.limit);

    const { rows } = await this.pool.query(query, values);
    if (shouldReverse) rows.reverse();
    return { items: rows, filters };
  }

  /**
   * Retrieves the meta data of a log entry.
   *
   * @async
   * @function getMeta
   * @param {number} id - The unique ID of the log entry.
   * @returns {Promise<{item: id, meta}>}  - A Promise that resolves with an object containing the log ID and its associated metadata.
   * @throws {Error} - Throws an error if the log entry is not found or the operation fails.
   */
  async getMeta (id) {
    const query = 'SELECT id, meta FROM errsole_logs_v2 WHERE id = $1';
    const { rows } = await this.pool.query(query, [id]);
    if (!rows.length) {
      throw new Error('Log entry not found.');
    }
    return { item: rows[0] };
  }

  /**
   * Deletes expired logs based on TTL configuration.
   *
   * @async
   * @function deleteExpiredLogs
   */
  async deleteExpiredLogs () {
    if (this.deleteExpiredLogsRunning) return;

    this.deleteExpiredLogsRunning = true;

    const DEFAULT_LOGS_TTL = 30 * 24 * 60 * 60 * 1000; // 30 days in milliseconds

    try {
      let logsTTL = DEFAULT_LOGS_TTL;
      const configResult = await this.getConfig('logsTTL');
      if (configResult.item) {
        const parsedTTL = parseInt(configResult.item.value, 10);
        logsTTL = isNaN(parsedTTL) ? DEFAULT_LOGS_TTL : parsedTTL;
      }
      const expirationTime = new Date(Date.now() - logsTTL).toISOString();
      let deletedRowCount;
      do {
        const result = await this.pool.query(
          `WITH deleted AS (
            SELECT id FROM errsole_logs_v2
            WHERE timestamp < $1 
            LIMIT 1000
          )
          DELETE FROM errsole_logs_v2 
          WHERE id IN (SELECT id FROM deleted)
          RETURNING *`,
          [expirationTime]
        );
        deletedRowCount = result.rowCount;
        await new Promise(resolve => setTimeout(resolve, 10000)); // Wait for 10 seconds before the next iteration
      } while (deletedRowCount > 0);
    } catch (err) {
      console.error(err);
    } finally {
      this.deleteExpiredLogsRunning = false;
    }
  }

  /**
   * Inserts a notification, counts today's notifications, and retrieves the previous notification.
   * @param {Notification} notification - The notification to be inserted.
   * @returns {Promise<Object>} - Returns today's notification count and the previous notification.
   */
  async insertNotificationItem (notification = {}) {
    const errsoleId = notification.errsole_id;
    const hostname = notification.hostname;
    const hashedMessage = notification.hashed_message;

    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const fetchPreviousNotificationQuery = `
        SELECT * FROM errsole_notifications
        WHERE hostname = $1 AND hashed_message = $2
        ORDER BY created_at DESC
        LIMIT 1;
      `;
      const { rows: [previousNotificationItem] } = await client.query(fetchPreviousNotificationQuery, [hostname, hashedMessage]);

      const insertNotificationQuery = `
        INSERT INTO errsole_notifications (hostname, errsole_id, hashed_message)
        VALUES ($1, $2, $3);
      `;
      await client.query(insertNotificationQuery, [hostname, errsoleId, hashedMessage]);

      const startOfDayUTC = new Date();
      startOfDayUTC.setUTCHours(0, 0, 0, 0);
      const endOfDayUTC = new Date();
      endOfDayUTC.setUTCHours(23, 59, 59, 999);
      const countTodayNotificationsQuery = `
        SELECT COUNT(*) AS notificationCount
        FROM errsole_notifications
        WHERE hashed_message = $1 AND created_at BETWEEN $2 AND $3;
      `;
      const { rows: [{ notificationcount: todayNotificationCount }] } = await client.query(countTodayNotificationsQuery, [hashedMessage, startOfDayUTC, endOfDayUTC]);

      await client.query('COMMIT');

      return {
        previousNotificationItem,
        todayNotificationCount
      };
    } catch (err) {
      console.error(err);
      await client.query('ROLLBACK');
    } finally {
      client.release();
    }
  }

  /**
   * Deletes expired notifications based on TTL configuration.
   *
   * @async
   * @function deleteExpiredNotificationItems
   */
  async deleteExpiredNotificationItems () {
    if (this.deleteExpiredNotificationItemsRunning) return;

    this.deleteExpiredNotificationItemsRunning = true;

    const DEFAULT_NOTIFICATIONS_TTL = 30 * 24 * 60 * 60 * 1000;

    try {
      let notificationsTTL = DEFAULT_NOTIFICATIONS_TTL;
      const configResult = await this.getConfig('logsTTL');
      if (configResult.item) {
        const parsedTTL = parseInt(configResult.item.value, 10);
        notificationsTTL = isNaN(parsedTTL) ? DEFAULT_NOTIFICATIONS_TTL : parsedTTL;
      }
      const expirationTime = new Date(Date.now() - notificationsTTL).toISOString();
      let deletedRowCount;
      do {
        const deleteQuery = `
          WITH deleted AS (
            SELECT id FROM errsole_notifications
            WHERE created_at < $1
            LIMIT 1000
          )
          DELETE FROM errsole_notifications
          WHERE id IN (SELECT id FROM deleted)
          RETURNING *;
        `;
        const { rowCount } = await this.pool.query(deleteQuery, [expirationTime]);
        deletedRowCount = rowCount;
        await new Promise(resolve => setTimeout(resolve, 10000));
      } while (deletedRowCount > 0);
    } catch (err) {
      console.error(err);
    } finally {
      this.deleteExpiredNotificationItemsRunning = false;
    }
  }

  /**
   * Creates a new user record in the database.
   *
   * @async
   * @function createUser
   * @param {Object} user - The user data.
   * @param {string} user.name - The name of the user.
   * @param {string} user.email - The email address of the user.
   * @param {string} user.password - The password of the user.
   * @param {string} user.role - The role of the user.
   * @returns {Promise<{item: User}>} - A promise that resolves with an object containing the new user item.
   * @throws {Error} - Throws an error if the user creation fails due to duplicate email or other database issues.
   */
  async createUser (user) {
    const SALT_ROUNDS = 10;
    const hashedPassword = await bcrypt.hash(user.password, SALT_ROUNDS);
    const query = 'INSERT INTO errsole_users (name, email, hashed_password, role) VALUES ($1, $2, $3, $4) RETURNING id';
    const values = [user.name, user.email, hashedPassword, user.role];
    try {
      const { rows } = await this.pool.query(query, values);
      return { item: { id: rows[0].id, name: user.name, email: user.email, role: user.role } };
    } catch (err) {
      if (err.code === '23505') {
        throw new Error('A user with the provided email already exists.');
      }
      throw err;
    }
  }

  /**
   * Verifies a user's credentials against stored records.
   *
   * @async
   * @function verifyUser
   * @param {string} email - The email address of the user.
   * @param {string} password - The password of the user
   * @returns {Promise<{item: User}>} - A promise that resolves with an object containing the user item upon successful verification.
   * @throws {Error} - Throws an error if the operation fails.
   */
  async verifyUser (email, password) {
    if (!email || !password) {
      throw new Error('Both email and password are required for verification.');
    }

    const query = 'SELECT * FROM errsole_users WHERE email = $1';
    const { rows } = await this.pool.query(query, [email]);
    if (!rows.length) throw new Error('User not found.');

    const user = rows[0];
    const isPasswordCorrect = await bcrypt.compare(password, user.hashed_password);
    if (!isPasswordCorrect) throw new Error('Incorrect password.');
    delete user.hashed_password;
    return { item: user };
  }

  /**
   * Retrieves the total count of users from the database.
   *
   * @async
   * @function getUserCount
   * @returns {Promise<{count: number}>} - A promise that resolves with an object containing the count of users.
   * @throws {Error} - Throws an error if the operation fails.
   */
  async getUserCount () {
    const query = 'SELECT COUNT(*) as count FROM errsole_users';
    const { rows } = await this.pool.query(query);
    return { count: parseInt(rows[0].count, 10) };
  }

  /**
   * Retrieves all user records from the database.
   *
   * @async
   * @function getAllUsers
   * @returns {Promise<{items: User[]}>} - A promise that resolves with an object containing an array of user items.
   * @throws {Error} - Throws an error if the operation fails.
   */
  async getAllUsers () {
    const query = 'SELECT id, name, email, role FROM errsole_users';
    const { rows } = await this.pool.query(query);
    return { items: rows };
  }

  /**
   * Retrieves a user record from the database based on the provided email.
   *
   * @async
   * @function getUserByEmail
   * @param {string} email - The email address of the user.
   * @returns {Promise<{item: User}>} - A Promise that resolves with an object containing the user item.
   * @throws {Error} - Throws an error if no user matches the email address.
   */
  async getUserByEmail (email) {
    if (!email) throw new Error('Email is required.');

    const query = 'SELECT id, name, email, role FROM errsole_users WHERE email = $1';
    const { rows } = await this.pool.query(query, [email]);
    if (!rows.length) throw new Error('User not found.');
    return { item: rows[0] };
  }

  /**
   * Updates a user's record in the database based on the provided email.
   *
   * @async
   * @function updateUserByEmail
   * @param {string} email - The email address of the user to be updated.
   * @param {Object} updates - The updates to be applied to the user record.
   * @returns {Promise<{item: User}>} - A Promise that resolves with an object containing the updated user item.
   * @throws {Error} - Throws an error if no updates could be applied or the user is not found.
   */
  async updateUserByEmail (email, updates) {
    if (!email) throw new Error('Email is required.');
    if (!updates || Object.keys(updates).length === 0) throw new Error('No updates provided.');

    const restrictedFields = ['id', 'hashed_password'];
    restrictedFields.forEach(field => delete updates[field]);

    const setClause = Object.keys(updates).map((key, i) => `${key} = $${i + 1}`).join(', ');
    const values = [...Object.values(updates), email];
    const query = `UPDATE errsole_users SET ${setClause} WHERE email = $${values.length}`;
    const result = await this.pool.query(query, values);
    if (result.rowCount === 0) throw new Error('No updates applied.');

    const updatedUser = await this.getUserByEmail(email);
    return updatedUser;
  }

  /**
   * Updates a user's password in the database.
   *
   * @async
   * @function updatePassword
   * @param {string} email - The email address of the user whose password is to be updated.
   * @param {string} currentPassword - The current password of the user for verification.
   * @param {string} newPassword - The new password to replace the current one.
   * @returns {Promise<{item: User}>} - A Promise that resolves with an object containing the updated user item (excluding sensitive information).
   * @throws {Error} - If the user is not found, if the current password is incorrect, or if the password update fails.
   */
  async updatePassword (email, currentPassword, newPassword) {
    if (!email || !currentPassword || !newPassword) {
      throw new Error('Email, current password, and new password are required.');
    }

    const query = 'SELECT * FROM errsole_users WHERE email = $1';
    const { rows } = await this.pool.query(query, [email]);
    if (!rows.length) throw new Error('User not found.');

    const user = rows[0];

    const isPasswordCorrect = await bcrypt.compare(currentPassword, user.hashed_password);
    if (!isPasswordCorrect) throw new Error('Current password is incorrect.');

    const hashedPassword = await bcrypt.hash(newPassword, 10);
    const updateQuery = 'UPDATE errsole_users SET hashed_password = $1 WHERE email = $2';
    const updateResult = await this.pool.query(updateQuery, [hashedPassword, email]);
    if (updateResult.rowCount === 0) throw new Error('Password update failed.');

    delete user.hashed_password;
    return { item: user };
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
    const result = await this.pool.query(query, [id]);
    if (result.rowCount === 0) throw new Error('User not found.');
    return {};
  }
}

module.exports = ErrsolePostgres;
module.exports.default = ErrsolePostgres;
