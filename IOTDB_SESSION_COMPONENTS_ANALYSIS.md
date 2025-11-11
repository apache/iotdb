# IoTDB Session 相关组件详解

## 1. 组件概览

IoTDB 中的 Session 相关组件分为**服务端**和**客户端**两部分：

### 1.1 服务端组件（Server-Side）

| 组件 | 位置 | 作用 |
|------|------|------|
| **SessionManager** | `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/SessionManager.java` | 服务端会话管理器（单例），管理所有客户端会话 |
| **IClientSession** | `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/IClientSession.java` | 服务端会话接口，定义会话的基本操作 |
| **ClientSession** | `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/ClientSession.java` | Thrift RPC 客户端会话实现 |
| **InternalClientSession** | `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/InternalClientSession.java` | 内部服务会话实现（CQ、Select Into） |
| **ClientRPCServiceImpl** | `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/thrift/impl/ClientRPCServiceImpl.java` | Thrift RPC 服务实现，处理客户端请求 |
| **SessionInfo** | `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/common/SessionInfo.java` | 会话信息的不可变数据传输对象 |
| **Coordinator** | `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/Coordinator.java` | 查询协调器，使用 SessionInfo 进行查询执行 |

### 1.2 客户端组件（Client-Side）

| 组件 | 位置 | 作用 |
|------|------|------|
| **Session** | `iotdb-client/session/src/main/java/org/apache/iotdb/session/Session.java` | 客户端会话，负责网络通信和请求发送 |
| **SessionPool** | `iotdb-client/session/src/main/java/org/apache/iotdb/session/pool/SessionPool.java` | 客户端会话池，管理多个 Session 实例 |

---

## 2. 服务端组件详解

### 2.1 SessionManager（会话管理器）

**作用**：服务端单例，管理所有客户端会话的生命周期。

**关键特性**：
- **单例模式**：通过 `SessionManager.getInstance()` 获取
- **ThreadLocal 存储**：`currSession: ThreadLocal<IClientSession>`，用于 client-thread 模型
- **Map 存储**：`sessions: Map<IClientSession, Object>`，全局会话映射
- **会话 ID 生成器**：`sessionIdGenerator: AtomicLong`
- **Statement ID 生成器**：`statementIdGenerator: AtomicLong`

**关键方法**：
```java
// 注册会话（在连接建立时调用）
public boolean registerSession(IClientSession session)

// 获取当前线程的会话
public IClientSession getCurrSession()

// 登录验证
public BasicOpenSessionResp login(...)

// 设置会话信息
public void supplySession(IClientSession session, long userId, String username, ZoneId zoneId, ...)

// 将会话信息转换为 SessionInfo（用于查询引擎）
public SessionInfo getSessionInfo(IClientSession session)
```

**代码位置**：
```71:103:d:/TsinghuaSE/iotdb/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/SessionManager.java
public class SessionManager implements SessionManagerMBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);
  private static final DNAuditLogger AUDIT_LOGGER = DNAuditLogger.getInstance();

  // When the client abnormally exits, we can still know who to disconnect
  /** currSession can be only used in client-thread model services. */
  private final ThreadLocal<IClientSession> currSession = new ThreadLocal<>();

  private final ThreadLocal<Long> currSessionIdleTime = new ThreadLocal<>();

  // sessions does not contain MqttSessions..
  private final Map<IClientSession, Object> sessions = new ConcurrentHashMap<>();
  // used for sessions.
  private final Object placeHolder = new Object();

  private final AtomicLong sessionIdGenerator = new AtomicLong();

  // The statementId is unique in one IoTDB instance.
  private final AtomicLong statementIdGenerator = new AtomicLong();

  public static final TSProtocolVersion CURRENT_RPC_VERSION =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

  protected SessionManager() {
    // singleton
    String mbeanName =
        String.format(
            "%s:%s=%s",
            IoTDBConstant.IOTDB_SERVICE_JMX_NAME,
            IoTDBConstant.JMX_TYPE,
            ServiceType.SESSION_MANAGER.getJmxName());
    JMXService.registerMBean(this, mbeanName);
  }
```

---

### 2.2 IClientSession（会话接口）

**作用**：定义服务端会话的基本操作和属性。

**关键属性**：
- `id`：会话 ID
- `userId`、`username`：用户身份信息
- `zoneId`、`timeZone`：时区信息
- `sqlDialect`：SQL 方言（TREE 或 TABLE）
- `databaseName`：当前数据库
- `statementIdToQueryId`：Statement 到 Query 的映射
- `preparedStatements`：Prepared Statements 映射（**我们新增的功能**）

**关键方法**：
```java
// Prepared Statements 管理（我们新增的）
void addPreparedStatement(String statementName, PreparedStatementInfo info)
PreparedStatementInfo removePreparedStatement(String statementName)
PreparedStatementInfo getPreparedStatement(String statementName)
Set<String> getPreparedStatementNames()
```

---

### 2.3 ClientSession（Thrift RPC 客户端会话）

**作用**：Thrift RPC 客户端连接的会话实现。

**关键特性**：
- **包装 Socket**：`private final Socket clientSocket`
- **连接 ID**：`"ip:port"`（如 `"192.168.1.100:6667"`）
- **连接类型**：`TSConnectionType.THRIFT_BASED`
- **Prepared Statements 存储**：`Map<String, PreparedStatementInfo> preparedStatements`

**代码位置**：
```29:61:d:/TsinghuaSE/iotdb/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/ClientSession.java
/** Client Session is the only identity for a connection. */
public class ClientSession extends IClientSession {

  private final Socket clientSocket;

  private final Map<Long, Set<Long>> statementIdToQueryId = new ConcurrentHashMap<>();
  
  // Map from statement name to PreparedStatementInfo
  private final Map<String, PreparedStatementInfo> preparedStatements = new ConcurrentHashMap<>();

  public ClientSession(Socket clientSocket) {
    this.clientSocket = clientSocket;
  }

  @Override
  public String getClientAddress() {
    return clientSocket.getInetAddress().getHostAddress();
  }

  @Override
  public int getClientPort() {
    return clientSocket.getPort();
  }

  @Override
  TSConnectionType getConnectionType() {
    return TSConnectionType.THRIFT_BASED;
  }

  @Override
  String getConnectionId() {
    return getClientAddress() + ':' + getClientPort();
  }
```

---

### 2.4 InternalClientSession（内部服务会话）

**作用**：用于内部服务（如 CQ、Select Into）的会话实现。

**关键特性**：
- **不依赖 Socket**：使用自定义 `clientID`（如 CQ ID）
- **连接 ID**：自定义字符串（如 `"cq_123"`）
- **连接类型**：`TSConnectionType.INTERNAL`
- **Prepared Statements 存储**：`Map<String, PreparedStatementInfo> preparedStatements`

**代码位置**：
```29:63:d:/TsinghuaSE/iotdb/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/InternalClientSession.java
/** For Internal usage, like CQ and Select Into. */
public class InternalClientSession extends IClientSession {

  // For CQ, it will be cq_id
  // For Select Into, it will be SELECT_INTO constant string
  private final String clientID;

  private final Map<Long, Set<Long>> statementIdToQueryId = new ConcurrentHashMap<>();
  
  // Map from statement name to PreparedStatementInfo
  private final Map<String, PreparedStatementInfo> preparedStatements = new ConcurrentHashMap<>();

  public InternalClientSession(String clientID) {
    this.clientID = clientID;
  }

  @Override
  public String getClientAddress() {
    return clientID;
  }

  @Override
  public int getClientPort() {
    return 0;
  }

  @Override
  TSConnectionType getConnectionType() {
    return TSConnectionType.INTERNAL;
  }

  @Override
  String getConnectionId() {
    return clientID;
  }
```

---

### 2.5 ClientRPCServiceImpl（Thrift RPC 服务实现）

**作用**：实现 `IClientRPCService` 接口，处理客户端 RPC 请求。

**关键特性**：
- **单例**：`private static final SessionManager SESSION_MANAGER = SessionManager.getInstance()`
- **获取当前会话**：`SESSION_MANAGER.getCurrSessionAndUpdateIdleTime()`
- **执行 SQL**：`executeStatementInternal()` 方法处理 SQL 执行请求

**关键方法**：
```java
// 打开会话
public TSOpenSessionResp openSession(TSOpenSessionReq req)

// 执行 SQL 语句
private TSExecuteStatementResp executeStatementInternal(TSExecuteStatementReq req, SelectResult setResult)
```

**代码位置**：
```231:306:d:/TsinghuaSE/iotdb/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/thrift/impl/ClientRPCServiceImpl.java
public class ClientRPCServiceImpl implements IClientRPCServiceWithHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientRPCServiceImpl.class);

  private static final DNAuditLogger AUDIT_LOGGER = DNAuditLogger.getInstance();

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private static final Logger SAMPLED_QUERIES_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.SAMPLED_QUERIES_LOGGER_NAME);

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  public static final String ERROR_CODE = "error code: ";

  private static final TSProtocolVersion CURRENT_RPC_VERSION =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

  private final IPartitionFetcher partitionFetcher;

  private final ISchemaFetcher schemaFetcher;

  private final Metadata metadata;

  private final SqlParser relationSqlParser;

  private final TsBlockSerde serde = new TsBlockSerde();

  private final TreeDeviceSchemaCacheManager DATA_NODE_SCHEMA_CACHE =
      TreeDeviceSchemaCacheManager.getInstance();

  public static final Duration DEFAULT_TIME_SLICE = new Duration(60_000, TimeUnit.MILLISECONDS);

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  @FunctionalInterface
  public interface SelectResult {

    boolean apply(TSExecuteStatementResp resp, IQueryExecution queryExecution, int fetchSize)
        throws IoTDBException, IOException;
  }

  private static final SelectResult SELECT_RESULT =
      (resp, queryExecution, fetchSize) -> {
        Pair<List<ByteBuffer>, Boolean> pair =
            QueryDataSetUtils.convertQueryResultByFetchSize(queryExecution, fetchSize);
        resp.setQueryResult(pair.left);
        return pair.right;
      };

  private static final SelectResult OLD_SELECT_RESULT =
      (resp, queryExecution, fetchSize) -> {
        Pair<TSQueryDataSet, Boolean> pair = convertTsBlockByFetchSize(queryExecution, fetchSize);
        resp.setQueryDataSet(pair.left);
        return pair.right;
      };

  public ClientRPCServiceImpl() {
    partitionFetcher = ClusterPartitionFetcher.getInstance();
    schemaFetcher = ClusterSchemaFetcher.getInstance();
    metadata = LocalExecutionPlanner.getInstance().metadata;
    relationSqlParser = new SqlParser();
  }

  private TSExecuteStatementResp executeStatementInternal(
      TSExecuteStatementReq req, SelectResult setResult) {
    boolean finished = false;
    Long statementId = req.getStatementId();
    long queryId = Long.MIN_VALUE;
    String statement = req.getStatement();
    IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
```

---

### 2.6 SessionInfo（会话信息数据传输对象）

**作用**：不可变的会话信息对象，用于在查询引擎中传递会话信息。

**关键特性**：
- **不可变**：所有字段都是 `final`
- **轻量级**：只包含查询引擎需要的信息（不包含 Socket、Prepared Statements 等）
- **可序列化**：支持序列化和反序列化（用于分布式查询）

**关键属性**：
- `sessionId`：会话 ID
- `userEntity`：用户实体（userId、username、cliHostname）
- `zoneId`：时区
- `databaseName`：数据库名称
- `sqlDialect`：SQL 方言
- `version`：客户端版本

**代码位置**：
```39:80:d:/TsinghuaSE/iotdb/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/common/SessionInfo.java
public class SessionInfo {
  private final long sessionId;
  private final UserEntity userEntity;
  private final ZoneId zoneId;

  @Nullable private final String databaseName;

  private final IClientSession.SqlDialect sqlDialect;

  private ClientVersion version = ClientVersion.V_1_0;

  public SessionInfo(long sessionId, UserEntity userEntity, ZoneId zoneId) {
    this.sessionId = sessionId;
    this.userEntity = userEntity;
    this.zoneId = zoneId;
    this.databaseName = null;
    this.sqlDialect = IClientSession.SqlDialect.TREE;
  }

  public SessionInfo(
      long sessionId,
      UserEntity userEntity,
      ZoneId zoneId,
      @Nullable String databaseName,
      IClientSession.SqlDialect sqlDialect) {
    this(sessionId, userEntity, zoneId, ClientVersion.V_1_0, databaseName, sqlDialect);
  }

  public SessionInfo(
      long sessionId,
      UserEntity userEntity,
      ZoneId zoneId,
      ClientVersion version,
      @Nullable String databaseName,
      IClientSession.SqlDialect sqlDialect) {
    this.sessionId = sessionId;
    this.userEntity = userEntity;
    this.zoneId = zoneId;
    this.version = version;
    this.databaseName = databaseName;
    this.sqlDialect = sqlDialect;
  }
```

**转换方法**：
```java
// SessionManager 提供转换方法
public SessionInfo getSessionInfo(IClientSession session) {
    return new SessionInfo(
        session.getId(),
        new UserEntity(session.getUserId(), session.getUsername(), session.getClientAddress()),
        session.getZoneId(),
        session.getClientVersion(),
        session.getDatabaseName(),
        session.getSqlDialect());
}
```

---

### 2.7 Coordinator（查询协调器）

**作用**：查询协调器，负责创建和执行查询。

**关键特性**：
- **使用 SessionInfo**：接收 `SessionInfo` 参数，而不是 `IClientSession`
- **不直接访问 SessionManager**：通过 `SessionInfo` 获取会话信息
- **创建查询执行**：`createQueryExecutionForTableModel()`、`executeForTreeModel()` 等

**关键方法**：
```java
// 为 Table Model 创建查询执行
public ExecutionResult createQueryExecutionForTableModel(
    QueryId queryId,
    SessionInfo session,
    String sql,
    Metadata metadata,
    long timeOut,
    boolean userQuery)

// 为 Tree Model 执行查询
public ExecutionResult executeForTreeModel(
    Statement statement,
    QueryId queryId,
    SessionInfo session,
    String sql,
    IPartitionFetcher partitionFetcher,
    ISchemaFetcher schemaFetcher,
    long timeOut,
    boolean userQuery)
```

**为什么使用 SessionInfo 而不是 IClientSession？**
- **解耦**：查询引擎不需要直接访问会话管理
- **轻量级**：只传递必要的信息
- **可序列化**：`SessionInfo` 可以序列化，用于分布式查询
- **线程安全**：`SessionInfo` 是不可变的

---

## 3. 客户端组件详解

### 3.1 Session（客户端会话）

**作用**：客户端会话，负责与 IoTDB 服务端建立连接、发送请求、接收响应。

**关键特性**：
- **网络通信**：使用 Thrift RPC 与服务端通信
- **连接管理**：管理 `SessionConnection`（Thrift 连接）
- **请求发送**：`executeStatement()`、`executeQueryStatement()` 等
- **不存储 Prepared Statements**：客户端不维护 prepared statements 状态（**服务端维护**）

**代码位置**：
```112:150:d:/TsinghuaSE/iotdb/iotdb-client/session/src/main/java/org/apache/iotdb/session/Session.java
public class Session implements ISession {

  private static final Logger logger = LoggerFactory.getLogger(Session.class);
  protected static final TSProtocolVersion protocolVersion =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;
  public static final String MSG_UNSUPPORTED_DATA_TYPE = "Unsupported data type:";
  public static final String MSG_DONOT_ENABLE_REDIRECT =
      "Query do not enable redirect," + " please confirm the session and server conf.";
  private static final ThreadPoolExecutor OPERATION_EXECUTOR =
      new ThreadPoolExecutor(
          SessionConfig.DEFAULT_SESSION_EXECUTOR_THREAD_NUM,
          SessionConfig.DEFAULT_SESSION_EXECUTOR_THREAD_NUM,
          0,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(SessionConfig.DEFAULT_SESSION_EXECUTOR_TASK_NUM),
          ThreadUtils.createThreadFactory("SessionExecutor", true));
  protected List<String> nodeUrls;
  protected String username;
  protected String password;
  protected int fetchSize;
  protected boolean useSSL;
  protected String trustStore;
  protected String trustStorePwd;

  /**
   * Timeout of query can be set by users. A negative number means using the default configuration
   * of server. And value 0 will disable the function of query timeout.
   */
  private long queryTimeoutInMs = -1;

  protected boolean enableThriftRpcCompaction;
  protected boolean enableIoTDBRpcCompression = true;
  protected int tabletCompressionMinRowSize = 10;
  protected int connectionTimeoutInMs;
  protected ZoneId zoneId;
  protected int thriftDefaultBufferSize;
  protected int thriftMaxFrameSize;
  protected TEndPoint defaultEndPoint;
  protected SessionConnection defaultSessionConnection;
```

---

### 3.2 SessionPool（客户端会话池）

**作用**：管理多个 `Session` 实例，提供连接池功能。

**关键特性**：
- **连接池**：`ConcurrentLinkedDeque<ISession> queue`，管理可用会话
- **占用会话**：`ConcurrentMap<ISession, ISession> occupied`，跟踪正在使用的会话
- **自动重连**：会话断开时自动创建新会话
- **线程安全**：支持多线程并发访问

**代码位置**：
```90:150:d:/TsinghuaSE/iotdb/iotdb-client/session/src/main/java/org/apache/iotdb/session/pool/SessionPool.java
public class SessionPool implements ISessionPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionPool.class);
  public static final String SESSION_POOL_IS_CLOSED = "Session pool is closed";
  public static final String CLOSE_THE_SESSION_FAILED = "close the session failed.";

  private static final int RETRY = 3;
  private static final int FINAL_RETRY = RETRY - 1;

  private final ConcurrentLinkedDeque<ISession> queue = new ConcurrentLinkedDeque<>();
  // for session whose resultSet is not released.
  private final ConcurrentMap<ISession, ISession> occupied = new ConcurrentHashMap<>();
  private int size = 0;
  private int maxSize = 0;
  private final long waitToGetSessionTimeoutInMs;

  // parameters for Session constructor
  private final String host;
  private final int port;
  private final String user;
  private final String password;
  private int fetchSize;

  private boolean useSSL;

  private String trustStore;

  private String trustStorePwd;
  private ZoneId zoneId;
  // this field only take effect in write request, nothing to do with any other type requests,
  // like query, load and so on.
  // if set to true, it means that we may redirect the write request to its corresponding leader
  // if set to false, it means that we will only send write request to first available DataNode(it
  // may be changed while current DataNode is not available, for example, we may retry to connect
  // to another available DataNode)
  // so even if enableRedirection is set to false, we may also send write request to another
  // datanode while encountering retriable errors in current DataNode
  private boolean enableRedirection;
  private boolean enableRecordsAutoConvertTablet;
  private boolean enableQueryRedirection = false;

  private Map<String, TEndPoint> deviceIdToEndpoint;
  private Map<IDeviceID, TEndPoint> tableModelDeviceIdToEndpoint;

  private int thriftDefaultBufferSize;
  private int thriftMaxFrameSize;

  /**
   * Timeout of query can be set by users. A negative number means using the default configuration
   * of server. And value 0 will disable the function of query timeout.
   */
  private long queryTimeoutInMs = -1;

  // The version number of the client which used for compatibility in the server
  private Version version;

  // parameters for Session#open()
  private final int connectionTimeoutInMs;
  private final boolean enableIoTDBRpcCompression;
  private final boolean enableThriftCompression;
```

---

## 4. 组件关系图

```
┌─────────────────────────────────────────────────────────────────┐
│                        客户端（Client）                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐         ┌──────────────┐                     │
│  │   Session    │────────>│ SessionPool  │                     │
│  │  (客户端会话)  │         │  (会话池)    │                     │
│  └──────────────┘         └──────────────┘                     │
│         │                                                       │
│         │ Thrift RPC                                           │
│         ▼                                                       │
└─────────┼───────────────────────────────────────────────────────┘
          │
          │ Thrift RPC
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                        服务端（Server）                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │              ClientRPCServiceImpl                          │ │
│  │         (Thrift RPC 服务实现)                               │ │
│  │                                                            │ │
│  │  executeStatementInternal()                               │ │
│  │    ├─> SESSION_MANAGER.getCurrSession()                    │ │
│  │    └─> COORDINATOR.executeForTableModel(sessionInfo)      │ │
│  └──────────────────────────────────────────────────────────┘ │
│                          │                                      │
│                          ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │              SessionManager                               │ │
│  │         (会话管理器，单例)                                  │ │
│  │                                                            │ │
│  │  - currSession: ThreadLocal<IClientSession>              │ │
│  │  - sessions: Map<IClientSession, Object>                │ │
│  │                                                            │ │
│  │  registerSession()                                        │ │
│  │  getCurrSession()                                         │ │
│  │  getSessionInfo() ──────────────────┐                     │ │
│  └──────────────────────────────────────┼─────────────────────┘ │
│                          │              │                       │
│                          ▼              │                       │
│  ┌──────────────────────────────────────┼─────────────────────┐ │
│  │         IClientSession               │                     │ │
│  │         (会话接口)                   │                     │ │
│  │                                      │                     │ │
│  │  - id, userId, username             │                     │ │
│  │  - zoneId, timeZone                 │                     │ │
│  │  - sqlDialect, databaseName         │                     │ │
│  │  - preparedStatements ──────────────┼─┐                   │ │
│  │    Map<String, PreparedStatementInfo>│ │                   │ │
│  └──────────────────────────────────────┼─┼───────────────────┘ │
│                          │              │ │                     │
│         ┌────────────────┴──┬───────────┘ │                     │
│         │                   │             │                     │
│         ▼                   ▼             │                     │
│  ┌──────────────┐   ┌─────────────────┐ │                     │
│  │ ClientSession│   │InternalClient    │ │                     │
│  │ (Thrift RPC) │   │Session (内部服务) │ │                     │
│  │              │   │                 │ │                     │
│  │ Socket       │   │ clientID        │ │                     │
│  │ "ip:port"    │   │ "cq_123"        │ │                     │
│  └──────────────┘   └─────────────────┘ │                     │
│                          │              │                       │
│                          └──────────────┘                       │
│                          │                                      │
│                          ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │              Coordinator                                 │ │
│  │         (查询协调器)                                      │ │
│  │                                                            │ │
│  │  createQueryExecutionForTableModel(SessionInfo)          │ │
│  │  executeForTreeModel(SessionInfo)                       │ │
│  └──────────────────────────────────────────────────────────┘ │
│                          │                                      │
│                          ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │              SessionInfo                                  │ │
│  │         (会话信息 DTO)                                     │ │
│  │                                                            │ │
│  │  - sessionId, userEntity, zoneId                         │ │
│  │  - databaseName, sqlDialect, version                    │ │
│  │  (不包含 preparedStatements)                              │ │
│  └──────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 5. 数据流：从客户端请求到查询执行

```
1. 客户端发送 SQL 请求
   Session.executeStatement() 
   └─> Thrift RPC: executeStatement(statement)

2. 服务端接收请求
   ClientRPCServiceImpl.executeStatementInternal()
   ├─> SESSION_MANAGER.getCurrSessionAndUpdateIdleTime()
   │   └─> 获取当前线程的 IClientSession
   │
   └─> COORDINATOR.executeForTableModel(...)
       ├─> SessionManager.getSessionInfo(clientSession)
       │   └─> 将 IClientSession 转换为 SessionInfo
       │
       └─> Coordinator.createQueryExecutionForTableModel(sessionInfo, sql, ...)
           └─> 使用 SessionInfo 创建查询执行
```

---

## 6. Prepared Statements 存储位置

### 6.1 存储位置

**Prepared Statements 存储在服务端的 `IClientSession` 中**：

| 类 | Prepared Statements 存储 |
|------|-------------------------|
| **ClientSession** | `Map<String, PreparedStatementInfo> preparedStatements` |
| **InternalClientSession** | `Map<String, PreparedStatementInfo> preparedStatements` |
| **MqttClientSession** | `Map<String, PreparedStatementInfo> preparedStatements` |
| **RestClientSession** | `Map<String, PreparedStatementInfo> preparedStatements` |

**代码位置**：
```36:37:d:/TsinghuaSE/iotdb/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/ClientSession.java
  private final Map<Long, Set<Long>> statementIdToQueryId = new ConcurrentHashMap<>();
  
  // Map from statement name to PreparedStatementInfo
  private final Map<String, PreparedStatementInfo> preparedStatements = new ConcurrentHashMap<>();
```

```38:39:d:/TsinghuaSE/iotdb/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/InternalClientSession.java
  private final Map<Long, Set<Long>> statementIdToQueryId = new ConcurrentHashMap<>();
  
  // Map from statement name to PreparedStatementInfo
  private final Map<String, PreparedStatementInfo> preparedStatements = new ConcurrentHashMap<>();
```

### 6.2 为什么存储在服务端？

1. **会话绑定**：Prepared statements 是会话级别的资源，应该与会话绑定
2. **安全性**：服务端可以验证 prepared statement 的权限和有效性
3. **一致性**：与 Trino 等数据库系统的设计一致
4. **简化客户端**：客户端不需要维护状态，只需要发送请求

### 6.3 访问 Prepared Statements

**在服务端访问**：
```java
// 在 PrepareTask 中
IClientSession session = configTaskExecutor.getCurrSession();
session.addPreparedStatement(statementName, info);

// 在 ExecuteTask 中（未来实现）
IClientSession session = configTaskExecutor.getCurrSession();
PreparedStatementInfo info = session.getPreparedStatement(statementName);
```

**注意**：`SessionInfo` **不包含** prepared statements，因为：
- `SessionInfo` 是轻量级的 DTO，只包含查询引擎需要的信息
- Prepared statements 只在会话管理层面使用，不需要传递给查询引擎
- 查询引擎执行时，prepared statement 已经被替换为实际 SQL

---

## 7. 关键区别总结

| 特性 | 服务端 Session | 客户端 Session |
|------|---------------|---------------|
| **位置** | `iotdb-core/datanode/.../session/` | `iotdb-client/session/.../Session.java` |
| **作用** | 管理服务端状态（用户、时区、prepared statements） | 网络通信和请求发送 |
| **存储** | `SessionManager` 中 | 客户端应用程序中 |
| **Prepared Statements** | ✅ 存储在 `IClientSession` 中 | ❌ 不存储 |
| **生命周期** | 从连接建立到断开 | 从应用程序创建到关闭 |
| **线程模型** | ThreadLocal（client-thread）或 Map（message-thread） | 单线程或线程池 |

---

## 8. 总结

1. **SessionManager**：服务端会话管理器，管理所有 `IClientSession`
2. **ClientSession**：Thrift RPC 客户端会话，包装 Socket
3. **InternalClientSession**：内部服务会话，不依赖 Socket
4. **ClientRPCServiceImpl**：Thrift RPC 服务实现，处理客户端请求
5. **SessionInfo**：会话信息 DTO，用于查询引擎（不包含 prepared statements）
6. **Coordinator**：查询协调器，使用 `SessionInfo` 进行查询执行
7. **客户端 Session**：客户端会话，负责网络通信
8. **SessionPool**：客户端会话池，管理多个 Session 实例

**Prepared Statements 存储**：
- ✅ **存储在服务端的 `IClientSession` 中**（`ClientSession`、`InternalClientSession` 等）
- ✅ **文件位置**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/ClientSession.java` 和 `InternalClientSession.java`
- ❌ **不存储在客户端**：客户端只负责发送请求，不维护 prepared statements 状态




