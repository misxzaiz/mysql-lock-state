const express = require('express');
const mysql = require('mysql2/promise');
const path = require('path');

const app = express();
const PORT = 3000;

// MySQL连接配置 - 请根据实际情况修改
const dbConfig = {
  host: 'localhost',
  user: 'root',
  password: '1234',
  database: 'performance_schema'
};

// 创建MySQL连接池
const pool = mysql.createPool({
  ...dbConfig,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// 静态文件服务
app.use(express.static(path.join(__dirname, 'public')));

// 获取锁状态信息
app.get('/api/locks', async (req, res) => {
  try {
    const connection = await pool.getConnection();
    
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
      locks,
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

app.listen(PORT, () => {
  console.log(`MySQL锁可视化工具运行在 http://localhost:${PORT}`);
  console.log('请确保MySQL服务正在运行，并检查数据库连接配置');
});