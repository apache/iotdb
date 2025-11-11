# IoTDB 会话（Session）实现原理与 PrepareTask 设计

## 1. 会话（Session）的基本概念

### 1.1 服务端 Session vs 客户端 Session

**服务端 Session (`IClientSession`)**：
- **位置**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/`
- **作用**：服务端用于管理每个客户端连接的状态信息
- **存储位置**：服务端的 `SessionManager` 中
- **生命周期**：从客户端连接到断开连接

**客户端 Session (`org.apache.iotdb.session.Session`)**：
- **位置**：`iotdb-client/session/src/main/java/org/apache/iotdb/session/`
- **作用**：客户端用于建立连接、发送请求、接收响应
- **存储位置**：客户端应用程序中
- **生命周期**：从客户端应用程序创建到关闭

**关键区别**：
- 服务端 Session 是**服务端代码**，管理服务端状态（用户信息、时区、数据库、prepared statements 等）
- 客户端 Session 是**客户端代码**，负责网络通信和请求发送
- 它们通过 Thrift RPC 协议进行通信

### 1.2 会话与连接的关系

**一个连接对应一个会话**（在 client-thread 模型中）：
- 当客户端建立 TCP 连接时，服务端创建 `ClientSession` 实例
- `ClientSession` 包装了 `Socket` 对象，通过 `getConnectionId()` 返回 "ip:port"
- `SessionManager` 使用 `ThreadLocal<IClientSession>` 存储当前线程的会话
- 连接断开时，会话被清理

## 2. 会话建立流程

### 2.1 代码流程

```
客户端连接
    ↓
BaseServerContextHandler.createContext()
    ├─ 从 Socket 创建 ClientSession
    └─ SessionManager.registerSession(clientSession)
        └─ 将 session 存储到 ThreadLocal 和 Map 中
    ↓
客户端调用 openSession RPC
    ↓
ClientRPCServiceImpl.openSession()
    ├─ 解析客户端版本、SQL 方言等
    └─ SessionManager.login()
        ├─ 验证用户名密码
        ├─ 检查版本兼容性
        └─ SessionManager.supplySession()
            ├─ 分配 sessionId
            ├─ 设置 userId、username
            ├─ 设置 zoneId、timeZone
            ├─ 设置 clientVersion
            └─ 标记为已登录
    ↓
返回 sessionId 给客户端
```

### 2.2 关键代码位置

**1. 会话创建** (`BaseServerContextHandler.java:58-69`):
```java
public ServerContext createContext(TProtocol in, TProtocol out) {
    Socket socket = ((TSocket) ((TElasticFramedTransport) out.getTransport()).getSocket()).getSocket();
    // 创建 ClientSession 并注册到 SessionManager
    getSessionManager().registerSession(new ClientSession(socket));
    // ...
}
```

**2. 会话注册** (`SessionManager.java:428-437`):
```java
public boolean registerSession(IClientSession session) {
    if (this.currSession.get() != null) {
        LOGGER.error("the client session is registered repeatedly, pls check whether this is a bug.");
        return false;
    }
    this.currSession.set(session);  // 存储到 ThreadLocal
    this.currSessionIdleTime.set(System.nanoTime());
    sessions.put(session, placeHolder);  // 存储到 Map
    return true;
}
```

**3. 会话登录** (`ClientRPCServiceImpl.java:1363-1394`):
```java
public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    IClientSession clientSession = SESSION_MANAGER.getCurrSession();
    BasicOpenSessionResp openSessionResp = SESSION_MANAGER.login(
        SESSION_MANAGER.getCurrSession(),
        req.username,
        req.password,
        req.zoneId,
        req.client_protocol,
        clientVersion,
        sqlDialect);
    // ...
}
```

**4. 会话信息设置** (`SessionManager.java:447-461`):
```java
public void supplySession(IClientSession session, long userId, String username, 
                         ZoneId zoneId, IoTDBConstant.ClientVersion clientVersion) {
    session.setId(sessionIdGenerator.incrementAndGet());  // 分配唯一 ID
    session.setUserId(userId);
    session.setUsername(username);
    session.setZoneId(zoneId);
    session.setClientVersion(clientVersion);
    session.setLogin(true);
    session.setLogInTime(System.currentTimeMillis());
}
```

## 3. 会话信息管理

### 3.1 会话存储结构

`SessionManager` 使用两种方式存储会话：

1. **ThreadLocal 存储** (`currSession: ThreadLocal<IClientSession>`)
   - 用于 client-thread 模型（Thrift RPC）
   - 每个线程有独立的会话
   - 通过 `getCurrSession()` 获取当前线程的会话

2. **Map 存储** (`sessions: Map<IClientSession, Object>`)
   - 全局会话映射
   - 用于会话管理和清理
   - 支持 MQTT 等 message-thread 模型

### 3.2 会话信息内容

`IClientSession` 存储的信息包括：
- **身份信息**：`id`、`userId`、`username`
- **连接信息**：`clientAddress`、`clientPort`、`connectionId`
- **配置信息**：`zoneId`、`timeZone`、`sqlDialect`、`databaseName`
- **状态信息**：`login`、`logInTime`、`clientVersion`
- **查询管理**：`statementIdToQueryId`（管理 statement 和 query 的映射）
- **Prepared Statements**：`preparedStatements`（我们新增的功能）

## 4. 不同 Session 类型的区别

### 4.1 ClientSession
- **用途**：Thrift RPC 客户端连接
- **特点**：包装 `Socket` 对象
- **连接 ID**：`"ip:port"`

### 4.2 InternalClientSession
- **用途**：内部服务（如 CQ、Select Into）
- **特点**：不依赖外部连接
- **连接 ID**：自定义字符串（如 CQ ID）

### 4.3 MqttClientSession
- **用途**：MQTT 协议客户端
- **特点**：message-thread 模型
- **连接 ID**：MQTT client ID

### 4.4 RestClientSession
- **用途**：REST API 客户端
- **特点**：HTTP 请求模型
- **连接 ID**：自定义 client ID

## 5. ClientRPCServiceImpl 的作用

`ClientRPCServiceImpl` 是 Thrift RPC 服务的实现类：
- **位置**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/thrift/impl/ClientRPCServiceImpl.java`
- **作用**：实现 `IClientRPCService` 接口，处理客户端 RPC 请求
- **关键方法**：
  - `openSession()`：处理会话建立
  - `executeStatement()`：处理 SQL 执行
  - `closeSession()`：处理会话关闭

**与 Session 的关系**：
- `ClientRPCServiceImpl` 通过 `SessionManager.getCurrSession()` 获取当前会话
- 使用会话信息进行权限检查、查询执行等操作
- 会话信息贯穿整个请求处理流程

## 6. PrepareTask 的设计思想

### 6.1 设计目标

`PrepareTask` 的设计目标是实现 `PREPARE` 语句的功能：
- 将 SQL 语句（带占位符）存储到会话中
- 为后续的 `EXECUTE` 语句提供基础

### 6.2 设计要点

**1. 会话绑定**：
- Prepared statement 存储在 `IClientSession` 中
- 每个会话有独立的 prepared statement 命名空间
- 会话断开时，prepared statements 自动清理

**2. 状态管理**：
- `PreparedStatementInfo` 存储：
  - `statementName`：prepared statement 名称
  - `sql`：SQL 语句的 AST（`Statement` 对象）
  - `zoneId`：创建时的时区
  - `createTime`：创建时间

**3. 执行流程**：
```
PREPARE stmt1 FROM SELECT * FROM table WHERE id = ?
    ↓
PrepareTask.execute()
    ├─ 获取当前会话（SessionManager.getCurrSession()）
    ├─ 创建 PreparedStatementInfo
    └─ 存储到会话（session.addPreparedStatement()）
```

### 6.3 为什么使用 SessionManager.getCurrSession()

**原因**：
1. **线程安全**：`ThreadLocal` 确保每个线程获取到正确的会话
2. **上下文隔离**：不同客户端的请求在不同线程中处理，不会混淆
3. **简化设计**：不需要传递会话参数，直接从上下文获取

**代码示例** (`PrepareTask.java:56`):
```java
IClientSession session = SessionManager.getInstance().getCurrSession();
if (session == null) {
    future.setException(
        new IllegalStateException("No current session available for PREPARE statement"));
    return future;
}
```

### 6.4 PrepareTask 的作用

1. **存储 Prepared Statement**：
   - 接收 `statementName`、`sql`（AST）、`zoneId`
   - 创建 `PreparedStatementInfo` 对象
   - 存储到当前会话的 `preparedStatements` Map 中

2. **处理重复定义**：
   - 如果同名 prepared statement 已存在，可以替换（类似 MySQL 行为）
   - 也可以选择抛出错误，取决于业务需求

3. **错误处理**：
   - 检查会话是否存在
   - 捕获异常并返回错误状态

## 7. 总结

- **服务端 Session** 是服务端代码，管理每个客户端连接的状态
- **一个连接对应一个会话**（在 client-thread 模型中）
- **会话建立流程**：连接 → 注册 → 登录 → 设置信息
- **会话信息**存储在 `ThreadLocal` 和 `Map` 中
- **PrepareTask** 利用会话存储 prepared statements，实现会话级别的状态管理





