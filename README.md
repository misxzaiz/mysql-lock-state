# MySQL锁状态可视化工具

一个简单的MySQL锁状态可视化工具，用于监控和分析MySQL数据库中的锁情况。

## 功能特性

- 实时显示MySQL锁信息
- 展示锁等待关系
- 监控活跃进程
- 自动刷新数据
- 简洁的Web界面

## 安装和运行

1. 安装依赖：
```bash
npm install
```

2. 修改数据库连接配置：
编辑 `server.js` 文件中的数据库连接信息：
```javascript
const dbConfig = {
  host: 'localhost',      // MySQL服务器地址
  user: 'root',           // 用户名
  password: '',           // 密码
  database: 'information_schema'
};
```

3. 启动服务：
```bash
npm start
```

4. 访问界面：
打开浏览器访问 `http://localhost:3000`

## 使用说明

- 页面会每5秒自动刷新数据
- 点击"刷新数据"按钮可手动刷新
- 红色高亮显示锁等待关系
- 显示当前活跃进程的SQL查询

## 技术栈

- 后端：Node.js + Express
- 前端：HTML + CSS + JavaScript
- 数据库：MySQL

## 注意事项

- 确保MySQL服务正在运行（仅支持MySQL 8.0+）
- 需要有足够的权限访问performance_schema和information_schema
- 建议在测试环境中使用
- MySQL 8.0+需要开启performance_schema（默认开启）