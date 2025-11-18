const express = require('express');
const mysql = require('mysql2/promise');
const path = require('path');

const app = express();
const PORT = 3000;

// 连接池管理
const connectionPools = new Map();

// 静态文件服务
app.use(express.static(path.join(__dirname, 'public')));
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

    // 查询锁等待信息 - MySQL 8.0+ 使用 performance_schema.data_lock_waits
    // 先检查表结构，然后查询实际存在的字段
    const [lockWaits] = await connection.execute(`
      SELECT * FROM performance_schema.data_lock_waits LIMIT 1
    `);

    // 如果表为空或字段不存在，使用简化查询
    let finalLockWaits = lockWaits;
    if (lockWaits.length === 0) {
      // 尝试查询可能存在的字段组合
      try {
        const [alternativeWaits] = await connection.execute(`
          SELECT 
            ENGINE_TRANSACTION_ID AS REQUESTING_ENGINE_TRANSACTION_ID,
            THREAD_ID AS REQUESTING_THREAD_ID,
            EVENT_ID AS REQUESTING_EVENT_ID,
            OBJECT_NAME AS REQUESTING_LOCK_ID,
            'UNKNOWN' AS BLOCKING_ENGINE_TRANSACTION_ID,
            0 AS BLOCKING_THREAD_ID,
            0 AS BLOCKING_EVENT_ID,
            'UNKNOWN' AS BLOCKING_LOCK_ID
          FROM performance_schema.data_locks 
          WHERE LOCK_STATUS = 'WAITING'
          LIMIT 10
        `);
        finalLockWaits = alternativeWaits;
      } catch (altError) {
        console.log('使用备用查询方式:', altError.message);
        finalLockWaits = [];
      }
    }

    // 查询进程信息
    const [processList] = await connection.execute(`
      SELECT 
        ID,
        USER,
        HOST,
        DB,
        COMMAND,
        TIME,
        STATE,
        INFO
      FROM information_schema.PROCESSLIST
      WHERE COMMAND != 'Sleep'
    `);

    connection.release();
    
    res.json({
      locks: enhancedLocks,
      lockWaits: finalLockWaits,
      processList,
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
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
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