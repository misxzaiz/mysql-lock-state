const express = require('express');
const mysql = require('mysql2/promise');
const path = require('path');

const app = express();
const PORT = 3000;

// 兼容exe环境的public目录路径
const isExe = process.pkg && process.execPath && process.execPath.endsWith('.exe');
const publicPath = isExe ? 
  path.join(path.dirname(process.execPath), 'public') : 
  path.join(__dirname, 'public');

// 连接池管理
const connectionPools = new Map();

// 静态文件服务
app.use(express.static(publicPath));
app.use(express.json());

// 生成唯一连接ID
function generateConnectionId() {
  return 'conn_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
}

// 创建连接池
function createConnectionPool(config) {
  return mysql.createPool({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    acquireTimeout: 60000,
    timeout: 60000
  });
}

// 测试连接
async function testConnection(config) {
  const testPool = createConnectionPool(config);
  try {
    const connection = await testPool.getConnection();
    await connection.ping();
    connection.release();
    await testPool.end();
    return true;
  } catch (error) {
    await testPool.end();
    throw error;
  }
}

// 锁范围解析函数
function parseLockRange(lock) {
  const result = {
    type: 'UNKNOWN',
    scope: '',
    description: '',
    icon: '❓',
    color: '#666'
  };

  // 解析锁类型
  if (lock.LOCK_TYPE === 'TABLE') {
    result.type = 'TABLE_LOCK';
    result.scope = '表级锁';
    result.description = `锁定整个表 ${lock.OBJECT_SCHEMA}.${lock.OBJECT_NAME}`;
    result.icon = '🔒';
    result.color = '#dc3545';
  } else if (lock.LOCK_TYPE === 'RECORD') {
    result.type = 'RECORD_LOCK';
    result.scope = '行级锁';
    
    // 解析锁模式
    if (lock.LOCK_MODE.includes('GAP')) {
      result.type = 'GAP_LOCK';
      result.scope = '间隙锁';
      result.icon = '🌊';
      result.color = '#fd7e14';
    } else if (lock.LOCK_MODE.includes('INSERT_INTENTION')) {
      result.type = 'INSERT_INTENTION_LOCK';
      result.scope = '插入意向锁';
      result.icon = '➕';
      result.color = '#20c997';
    } else {
      result.icon = '📍';
      result.color = '#007bff';
    }

    // 构建范围描述
    let rangeDesc = `表 ${lock.OBJECT_SCHEMA}.${lock.OBJECT_NAME}`;
    if (lock.INDEX_NAME && lock.INDEX_NAME !== 'PRIMARY') {
      rangeDesc += ` (索引: ${lock.INDEX_NAME})`;
    }
    
    if (lock.LOCK_DATA) {
      if (lock.LOCK_DATA.includes('supremum pseudo-record')) {
        rangeDesc += ' - 最大值之后的间隙';
      } else if (lock.LOCK_DATA.includes('infimum pseudo-record')) {
        rangeDesc += ' - 最小值之前的间隙';
      } else {
        rangeDesc += ` - 键值: ${lock.LOCK_DATA}`;
      }
    }
    
    result.description = rangeDesc;
  }

  // 添加锁模式信息
  if (lock.LOCK_MODE) {
    result.lockMode = lock.LOCK_MODE;
    if (lock.LOCK_MODE.includes('X')) {
      result.mode = '排他锁';
    } else if (lock.LOCK_MODE.includes('S')) {
      result.mode = '共享锁';
    } else if (lock.LOCK_MODE.includes('IS')) {
      result.mode = '意向共享锁';
    } else if (lock.LOCK_MODE.includes('IX')) {
      result.mode = '意向排他锁';
    }
  }

  return result;
}

// 创建连接
app.post('/api/connections', async (req, res) => {
  try {
    const config = req.body;
    
    // 验证配置
    if (!config.host || !config.user || !config.database) {
      return res.status(400).json({ 
        error: '配置不完整', 
        message: '请提供主机、用户名和数据库名' 
      });
    }
    
    // 测试连接
    try {
      await testConnection(config);
    } catch (error) {
      return res.status(400).json({ 
        error: '连接测试失败', 
        message: error.message 
      });
    }
    
    // 创建连接池
    const connectionId = generateConnectionId();
    const pool = createConnectionPool(config);
    connectionPools.set(connectionId, {
      pool,
      config,
      createdAt: new Date(),
      lastUsed: new Date()
    });
    
    console.log(`创建新连接: ${connectionId} -> ${config.host}:${config.port}/${config.database}`);
    
    res.json({ 
      connectionId,
      message: '连接成功' 
    });
    
  } catch (error) {
    console.error('创建连接失败:', error);
    res.status(500).json({ 
      error: '创建连接失败', 
      message: error.message 
    });
  }
});

// 删除连接
app.delete('/api/connections/:connectionId', async (req, res) => {
  try {
    const { connectionId } = req.params;
    const connectionInfo = connectionPools.get(connectionId);
    
    if (!connectionInfo) {
      return res.status(404).json({ 
        error: '连接不存在' 
      });
    }
    
    // 关闭连接池
    await connectionInfo.pool.end();
    connectionPools.delete(connectionId);
    
    console.log(`删除连接: ${connectionId}`);
    
    res.json({ 
      message: '连接已断开' 
    });
    
  } catch (error) {
    console.error('删除连接失败:', error);
    res.status(500).json({ 
      error: '删除连接失败', 
      message: error.message 
    });
  }
});

// 强制释放锁操作
app.post('/api/operations/kill-process', async (req, res) => {
  try {
    const { connectionId, threadId, type = 'connection' } = req.body;
    
    if (!connectionId || !threadId) {
      return res.status(400).json({ 
        error: '参数不完整', 
        message: '请提供connectionId和threadId' 
      });
    }
    
    const connectionInfo = connectionPools.get(connectionId);
    if (!connectionInfo) {
      return res.status(404).json({ 
        error: '连接不存在' 
      });
    }
    
    const connection = await connectionInfo.pool.getConnection();
    
    try {
      // 通过performance_schema.threads映射THREAD_ID到PROCESSLIST_ID
      const [threadMapping] = await connection.execute(`
        SELECT PROCESSLIST_ID 
        FROM performance_schema.threads 
        WHERE THREAD_ID = ?
      `, [threadId]);
      
      if (threadMapping.length === 0) {
        return res.status(404).json({ 
          error: '线程不存在', 
          message: `无法找到THREAD_ID ${threadId} 对应的进程` 
        });
      }
      
      const processlistId = threadMapping[0].PROCESSLIST_ID;
      
      // 验证进程是否仍然存在
      const [processCheck] = await connection.execute(`
        SELECT ID 
        FROM information_schema.PROCESSLIST 
        WHERE ID = ?
      `, [processlistId]);
      
      if (processCheck.length === 0) {
        return res.status(404).json({ 
          error: '进程已结束', 
          message: `PROCESSLIST_ID ${processlistId} 已不存在，可能已经自动结束` 
        });
      }
      
      // 执行KILL命令
      const killCommand = type === 'connection' ? `KILL ${processlistId}` : `KILL QUERY ${processlistId}`;
      await connection.execute(killCommand);
      
      console.log(`执行操作: ${killCommand} (连接: ${connectionId}, THREAD_ID: ${threadId} -> PROCESSLIST_ID: ${processlistId})`);
      
      res.json({ 
        success: true,
        message: `✅ 强制释放锁成功!\n\n执行操作: ${type === 'connection' ? 'KILL CONNECTION' : 'KILL QUERY'}\n线程映射: THREAD_ID ${threadId} → PROCESSLIST_ID ${processlistId}\n\n所有相关锁已被强制释放，事务已回滚。`,
        threadId,
        processlistId,
        type
      });
      
    } catch (error) {
      res.status(500).json({ 
        error: 'Kill操作失败', 
        message: error.message 
      });
    } finally {
      connection.release();
    }
    
  } catch (error) {
    console.error('Kill进程操作失败:', error);
    res.status(500).json({ 
      error: '操作失败', 
      message: error.message 
    });
  }
});

// 获取连接状态
app.get('/api/connections/:connectionId/status', async (req, res) => {
  try {
    const { connectionId } = req.params;
    const connectionInfo = connectionPools.get(connectionId);
    
    if (!connectionInfo) {
      return res.status(404).json({ 
        error: '连接不存在' 
      });
    }
    
    // 测试连接状态
    try {
      const connection = await connectionInfo.pool.getConnection();
      await connection.ping();
      connection.release();
      
      connectionInfo.lastUsed = new Date();
      
      res.json({ 
        status: 'connected',
        config: {
          host: connectionInfo.config.host,
          database: connectionInfo.config.database
        },
        createdAt: connectionInfo.createdAt,
        lastUsed: connectionInfo.lastUsed
      });
    } catch (error) {
      res.status(500).json({ 
        error: '连接异常', 
        message: error.message 
      });
    }
    
  } catch (error) {
    console.error('获取连接状态失败:', error);
    res.status(500).json({ 
      error: '获取连接状态失败', 
      message: error.message 
    });
  }
});

// 获取锁状态信息
app.get('/api/locks', async (req, res) => {
  try {
    const { connectionId } = req.query;
    
    if (!connectionId) {
      return res.status(400).json({ 
        error: '缺少连接ID', 
        message: '请提供connectionId参数' 
      });
    }
    
    const connectionInfo = connectionPools.get(connectionId);
    if (!connectionInfo) {
      return res.status(404).json({ 
        error: '连接不存在', 
        message: '请先建立数据库连接' 
      });
    }
    
    const connection = await connectionInfo.pool.getConnection();
    connectionInfo.lastUsed = new Date();
    
    // 查询当前锁信息 - MySQL 8.0+ 使用 performance_schema.data_locks
    const [locks] = await connection.execute(`
      SELECT 
        ENGINE,
        ENGINE_TRANSACTION_ID,
        THREAD_ID,
        OBJECT_SCHEMA,
        OBJECT_NAME,
        INDEX_NAME,
        LOCK_TYPE,
        LOCK_MODE,
        LOCK_STATUS,
        LOCK_DATA
      FROM performance_schema.data_locks
      ORDER BY ENGINE_TRANSACTION_ID, THREAD_ID
    `);

    // 为每个锁添加范围解析信息
    const enhancedLocks = locks.map(lock => ({
      ...lock,
      lockRange: parseLockRange(lock)
    }));

    // 查询锁等待信息 - 先尝试使用 data_lock_waits 表
    let finalLockWaits = [];
    try {
      // 先检查表结构
      const [columns] = await connection.execute(`
        SHOW COLUMNS FROM performance_schema.data_lock_waits
      `);
      console.log('data_lock_waits 表字段:', columns.map(c => c.Field));
      
      // 根据实际字段构建查询
      const [lockWaits] = await connection.execute(`
        SELECT 
          REQUESTING_ENGINE_TRANSACTION_ID,
          REQUESTING_THREAD_ID,
          REQUESTING_EVENT_ID,
          BLOCKING_ENGINE_TRANSACTION_ID,
          BLOCKING_THREAD_ID,
          BLOCKING_EVENT_ID
        FROM performance_schema.data_lock_waits
      `);
      finalLockWaits = lockWaits;
    } catch (waitError) {
      console.log('data_lock_waits 表查询失败，使用替代方案:', waitError.message);
      
      // 替代方案：通过分析锁状态识别等待关系
      try {
        // 查询处于等待状态的锁，并尝试找到阻塞的锁
        const [waitingLocks] = await connection.execute(`
          SELECT 
            w.ENGINE_TRANSACTION_ID AS REQUESTING_ENGINE_TRANSACTION_ID,
            w.THREAD_ID AS REQUESTING_THREAD_ID,
            w.EVENT_ID AS REQUESTING_EVENT_ID,
            CONCAT(w.OBJECT_SCHEMA, '.', w.OBJECT_NAME, ':', IFNULL(w.LOCK_DATA, '')) AS REQUESTING_LOCK_ID,
            b.ENGINE_TRANSACTION_ID AS BLOCKING_ENGINE_TRANSACTION_ID,
            b.THREAD_ID AS BLOCKING_THREAD_ID,
            b.EVENT_ID AS BLOCKING_EVENT_ID,
            CONCAT(b.OBJECT_SCHEMA, '.', b.OBJECT_NAME, ':', IFNULL(b.LOCK_DATA, '')) AS BLOCKING_LOCK_ID
          FROM performance_schema.data_locks w
          JOIN performance_schema.data_locks b ON (
            w.OBJECT_SCHEMA = b.OBJECT_SCHEMA 
            AND w.OBJECT_NAME = b.OBJECT_NAME
            AND IFNULL(w.INDEX_NAME, '') = IFNULL(b.INDEX_NAME, '')
            AND IFNULL(w.LOCK_DATA, '') = IFNULL(b.LOCK_DATA, '')
            AND w.ENGINE_TRANSACTION_ID != b.ENGINE_TRANSACTION_ID
            AND w.LOCK_STATUS = 'WAITING'
            AND b.LOCK_STATUS = 'GRANTED'
          )
          ORDER BY w.ENGINE_TRANSACTION_ID
        `);
        finalLockWaits = waitingLocks;
      } catch (altError) {
        console.log('替代查询也失败:', altError.message);
        finalLockWaits = [];
      }
    }

    // 查询进程信息，包含锁占用时间（移除Sleep限制，获取所有进程）
    const [processList] = await connection.execute(`
      SELECT 
        p.ID,
        p.USER,
        p.HOST,
        p.DB,
        p.COMMAND,
        p.TIME,
        p.STATE,
        p.INFO,
        IFNULL(t.PROCESSLIST_TIME, p.TIME) as PROCESSLIST_TIME
      FROM information_schema.PROCESSLIST p
      LEFT JOIN performance_schema.threads t ON p.ID = t.PROCESSLIST_ID
    `);

    // 查询事务信息和线程映射，获取锁占用时间
    const [transactions] = await connection.execute(`
      SELECT 
        TRX_ID,
        TRX_MYSQL_THREAD_ID,
        TRX_STARTED,
        TIMESTAMPDIFF(SECOND, TRX_STARTED, NOW()) as TRX_DURATION,
        TRX_STATE,
        TRX_REQUESTED_LOCK_ID,
        TRX_WAIT_STARTED
      FROM information_schema.INNODB_TRX
    `);

    // 查询线程映射关系
    const [threadMappings] = await connection.execute(`
      SELECT 
        THREAD_ID,
        PROCESSLIST_ID,
        NAME
      FROM performance_schema.threads
      WHERE PROCESSLIST_ID IS NOT NULL
    `);

    // 尝试多种方式获取SQL语句
    let recentSqls = [];
    let currentSqls = [];
    
    // 方法1：使用performance_schema.events_statements_history_long
    try {
      const [sqlHistory] = await connection.execute(`
        SELECT 
          THREAD_ID,
          EVENT_ID,
          TIMER_START,
          TIMER_END,
          SQL_TEXT,
          DIGEST_TEXT,
          CURRENT_SCHEMA
        FROM performance_schema.events_statements_history_long
        WHERE SQL_TEXT IS NOT NULL AND SQL_TEXT != ''
        ORDER BY TIMER_END DESC
        LIMIT 1000
      `);
      recentSqls = sqlHistory;
      console.log(`方法1: 查询到 ${sqlHistory.length} 条历史SQL记录`);
    } catch (sqlError) {
      console.log('方法1查询SQL历史失败:', sqlError.message);
      
      // 方法2：使用information_schema.processlist
      try {
        const [processSql] = await connection.execute(`
          SELECT 
            ID as THREAD_ID,
            INFO as SQL_TEXT,
            DB as CURRENT_SCHEMA,
            TIME,
            USER,
            HOST
          FROM information_schema.PROCESSLIST
          WHERE INFO IS NOT NULL AND INFO != '' AND COMMAND != 'Sleep'
          ORDER BY TIME DESC
          LIMIT 1000
        `);
        recentSqls = processSql.map(row => ({
          ...row,
          THREAD_ID: row.THREAD_ID,
          SQL_TEXT: row.SQL_TEXT,
          CURRENT_SCHEMA: row.CURRENT_SCHEMA
        }));
        console.log(`方法2: 查询到 ${processSql.length} 条进程SQL记录`);
      } catch (processError) {
        console.log('方法2查询进程SQL失败:', processError.message);
      }
    }
    
    // 查询当前正在执行的SQL
    try {
      const [currentSql] = await connection.execute(`
        SELECT 
          THREAD_ID,
          EVENT_ID,
          SQL_TEXT,
          DIGEST_TEXT,
          CURRENT_SCHEMA
        FROM performance_schema.events_statements_current
        WHERE SQL_TEXT IS NOT NULL AND SQL_TEXT != ''
        ORDER BY TIMER_START DESC
        LIMIT 1000
      `);
      currentSqls = currentSql;
      console.log(`查询到 ${currentSql.length} 条当前SQL记录`);
    } catch (sqlError) {
      console.log('查询当前SQL失败:', sqlError.message);
    }

    // 为锁信息添加时间数据和SQL历史
    const enhancedLocksWithTime = enhancedLocks.map(lock => {
      // 通过线程映射找到正确的PROCESSLIST_ID
      const threadMapping = threadMappings.find(t => t.THREAD_ID == lock.THREAD_ID);
      const processlistId = threadMapping ? threadMapping.PROCESSLIST_ID : null;
      
      // 通过PROCESSLIST_ID找到事务
      const transaction = processlistId ? 
        transactions.find(t => t.TRX_MYSQL_THREAD_ID == processlistId) : null;
      
      // 通过PROCESSLIST_ID找到进程信息
      const process = processlistId ? 
        processList.find(p => p.ID == processlistId) : null;

      // 查找相关的SQL语句
      let relatedSqls = [];
      
      // 1. 首先查找当前正在执行的SQL
      const currentThreadSqls = currentSqls.filter(sql => sql.THREAD_ID === lock.THREAD_ID);
      if (currentThreadSqls.length > 0) {
        relatedSqls = currentThreadSqls;
      }
      
      // 2. 如果没有当前SQL，查找历史SQL
      if (relatedSqls.length === 0) {
        const historyThreadSqls = recentSqls.filter(sql => sql.THREAD_ID === lock.THREAD_ID);
        if (historyThreadSqls.length > 0) {
          relatedSqls = historyThreadSqls.slice(0, 5); // 取最近5条
        }
      }
      
      // 3. 通过事务ID查找SQL（如果事务ID存在）
      if (relatedSqls.length === 0 && lock.ENGINE_TRANSACTION_ID) {
        // 通过线程ID关联事务
        const trxThread = threadMappings.find(t => {
          const trx = transactions.find(tr => tr.TRX_MYSQL_THREAD_ID === t.PROCESSLIST_ID);
          return trx && trx.TRX_ID === lock.ENGINE_TRANSACTION_ID;
        });
        
        if (trxThread) {
          const trxSqls = [...currentSqls, ...recentSqls].filter(sql => sql.THREAD_ID === trxThread.THREAD_ID);
          if (trxSqls.length > 0) {
            relatedSqls = trxSqls.slice(0, 5);
          }
        }
      }

      // 格式化SQL信息
      const sqlInfo = relatedSqls.length > 0 ? {
        current: relatedSqls[0].SQL_TEXT,
        digest: relatedSqls[0].DIGEST_TEXT,
        schema: relatedSqls[0].CURRENT_SCHEMA,
        history: relatedSqls.map(sql => ({
          text: sql.SQL_TEXT,
          digest: sql.DIGEST_TEXT,
          schema: sql.CURRENT_SCHEMA,
          timer: sql.TIMER_END
        }))
      } : null;

      return {
        ...lock,
        processlistId: processlistId,
        lockDuration: transaction ? transaction.TRX_DURATION : (process ? process.TIME : 0),
        trxStarted: transaction ? transaction.TRX_STARTED : null,
        trxState: transaction ? transaction.TRX_STATE : null,
        processTime: process ? process.TIME : 0,
        hasTransaction: !!transaction,
        threadMapping: !!threadMapping,
        sql: process ? process.INFO : (sqlInfo ? sqlInfo.current : null),  // 优先使用当前进程的SQL，然后是历史SQL
        sqlInfo: sqlInfo,  // 保存完整的SQL信息
        user: process ? process.USER : null, // 添加用户信息
        host: process ? process.HOST : null, // 添加主机信息
        db: process ? process.DB : (sqlInfo ? sqlInfo.schema : null), // 优先使用进程DB，然后是SQL schema
        command: process ? process.COMMAND : null, // 添加命令类型
        state: process ? process.STATE : null // 添加进程状态
      };
    });

    connection.release();
    
    res.json({
      locks: enhancedLocksWithTime,
      lockWaits: finalLockWaits,
      processList,
      transactions,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('获取锁信息失败:', error);
    res.status(500).json({ 
      error: '获取锁信息失败', 
      message: error.message 
    });
  }
});

// 主页路由
app.get('/', (req, res) => {
  res.sendFile(path.join(publicPath, 'index.html'));
});

// 定期清理未使用的连接
setInterval(() => {
  const now = new Date();
  const timeout = 30 * 60 * 1000; // 30分钟超时
  
  for (const [connectionId, connectionInfo] of connectionPools.entries()) {
    if (now - connectionInfo.lastUsed > timeout) {
      console.log(`清理超时连接: ${connectionId}`);
      connectionInfo.pool.end().catch(console.error);
      connectionPools.delete(connectionId);
    }
  }
}, 5 * 60 * 1000); // 每5分钟检查一次

app.listen(PORT, () => {
  console.log(`MySQL锁可视化工具运行在 http://localhost:${PORT}`);
  console.log('支持动态连接配置，请在前端界面中配置数据库连接');
});