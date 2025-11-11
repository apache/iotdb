# QueryStateMachine vs Session 设计分析

## 1. IoTDB 中的 QueryStateMachine

### 1.1 当前实现

IoTDB 中确实存在 `QueryStateMachine` 类，位于：
- **位置**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/QueryStateMachine.java`
- **作用**：跟踪查询执行状态（QUEUED, PLANNED, DISPATCHING, RUNNING, FINISHED等）
- **特点**：**只管理查询状态**，不管理 prepared statements

```java
public class QueryStateMachine {
    private final StateMachine<QueryState> queryState;
    private final AtomicReference<Throwable> failureException = new AtomicReference<>();
    private final AtomicReference<TSStatus> failureStatus = new AtomicReference<>();
    
    // 只跟踪查询状态，不跟踪 prepared statements
}
```

### 1.2 与 Trino 的区别

| 特性 | IoTDB QueryStateMachine | Trino QueryStateMachine |
|------|------------------------|-------------------------|
| **查询状态跟踪** | ✅ 是 | ✅ 是 |
| **Prepared Statements 跟踪** | ❌ 否 | ✅ 是 |
| **Session 属性跟踪** | ❌ 否 | ✅ 是 |
| **事务跟踪** | ❌ 否 | ✅ 是 |
| **查询信息构建** | ❌ 否 | ✅ 是 |

## 2. Trino 的设计：为什么存储在 QueryStateMachine 而不是 Session？

### 2.1 核心设计理念

Trino 采用**双层存储架构**：

```
┌─────────────────────────────────────────┐
│  Session (持久化存储)                      │
│  - 存储所有已提交的 prepared statements    │
│  - 跨查询持久化                            │
└─────────────────────────────────────────┘
              ↑ 延迟提交
              │
┌─────────────────────────────────────────┐
│  QueryStateMachine (查询级临时存储)        │
│  - 跟踪本次查询的增量变化                  │
│  - addedPreparedStatements               │
│  - deallocatedPreparedStatements         │
└─────────────────────────────────────────┘
```

### 2.2 关键好处

#### 好处 1：事务性和原子性

**问题场景**：
```
查询 1: PREPARE stmt1 FROM SELECT * FROM table1
查询 2: PREPARE stmt2 FROM SELECT * FROM table2
查询 3: EXECUTE stmt1 USING 100
```

如果查询 2 失败，stmt1 不应该受到影响。

**Trino 的解决方案**：
```java
// 在 QueryStateMachine 中跟踪
queryStateMachine.addPreparedStatement("stmt1", sql1);  // 查询 1
queryStateMachine.addPreparedStatement("stmt2", sql2);  // 查询 2（失败）

// 只有查询成功时，才提交到 Session
if (querySucceeded) {
    // 提交到 Session（实际在 QueryInfo 构建时完成）
    session.preparedStatements.putAll(addedPreparedStatements);
}
```

**IoTDB 当前实现的问题**：
```java
// 直接修改 Session，无法回滚
session.addPreparedStatement("stmt1", info1);  // 立即生效
session.addPreparedStatement("stmt2", info2);  // 立即生效，即使查询失败
```

#### 好处 2：查询级别的状态跟踪

**场景**：需要知道每个查询创建/删除了哪些 prepared statements

**Trino 的实现**：
```java
// QueryInfo 包含本次查询的所有变更
public class QueryInfo {
    private final Map<String, String> addedPreparedStatements;
    private final Set<String> deallocatedPreparedStatements;
    
    // 可以用于：
    // 1. 查询历史记录
    // 2. 审计日志
    // 3. 查询结果展示
}
```

**IoTDB 当前实现**：
- 无法知道某个查询创建了哪些 prepared statements
- 无法进行查询级别的审计

#### 好处 3：延迟提交和错误恢复

**场景**：查询执行过程中发生错误

**Trino 的流程**：
```
PREPARE stmt1
    ↓
QueryStateMachine.addPreparedStatement("stmt1", sql)
    ↓
查询执行...
    ↓
如果成功 → QueryInfo 包含 addedPreparedStatements → 客户端可以看到变更
如果失败 → QueryStateMachine 被丢弃 → Session 不受影响
```

**IoTDB 当前实现**：
```
PREPARE stmt1
    ↓
session.addPreparedStatement("stmt1", info)  // 立即生效
    ↓
查询执行...
    ↓
如果失败 → stmt1 已经存在于 Session 中 → 需要手动清理
```

#### 好处 4：查询信息完整性

**Trino 的 QueryInfo**：
```java
public class QueryInfo {
    // 查询基本信息
    private final QueryId queryId;
    private final QueryState state;
    
    // 本次查询的变更
    private final Map<String, String> addedPreparedStatements;
    private final Set<String> deallocatedPreparedStatements;
    private final Map<String, String> setSessionProperties;
    private final Set<String> resetSessionProperties;
    
    // 可以完整地记录查询的所有影响
}
```

**好处**：
- 查询历史可以完整记录
- 支持查询回放和调试
- 便于监控和审计

### 2.3 Trino 的实际工作流程

#### PREPARE 语句执行流程：

```java
// 1. PrepareTask.execute()
public ListenableFuture<Void> execute(Prepare prepare, QueryStateMachine stateMachine, ...) {
    String sql = getFormattedSql(statement, sqlParser);
    // 存储到 QueryStateMachine，而不是直接存储到 Session
    stateMachine.addPreparedStatement(prepare.getName().getValue(), sql);
    return immediateVoidFuture();
}

// 2. QueryStateMachine 跟踪变更
private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();

public void addPreparedStatement(String key, String value) {
    addedPreparedStatements.put(key, value);
    // 注意：这里不直接修改 Session
}

// 3. 查询完成时，构建 QueryInfo
public QueryInfo getQueryInfo(...) {
    return new QueryInfo(
        queryId,
        session.toSessionRepresentation(),  // Session 的快照
        state,
        // ...
        addedPreparedStatements,           // 本次查询的变更
        deallocatedPreparedStatements
    );
}

// 4. 客户端收到 QueryInfo，可以看到变更
// 5. 如果需要持久化，客户端可以基于 QueryInfo 更新本地 Session
```

**关键点**：
- QueryStateMachine 是**查询级别的临时存储**
- Session 是**持久化的全局存储**
- 两者分离，支持事务性和查询级别的跟踪

## 3. IoTDB 应该如何设计？

### 3.1 设计目标

1. **保持 IoTDB 的优势**：AST 存储，避免重复解析
2. **借鉴 Trino 的优势**：查询级别的状态跟踪，事务性支持
3. **适配 IoTDB 的架构**：ConfigExecution vs QueryExecution

### 3.2 推荐设计方案

#### 方案 A：扩展现有 QueryStateMachine（推荐）

**优点**：
- 最小化改动
- 保持架构一致性
- 利用现有的状态机机制

**实现**：

```java
// 扩展 IoTDB 的 QueryStateMachine
public class QueryStateMachine {
    private final StateMachine<QueryState> queryState;
    private final AtomicReference<Throwable> failureException = new AtomicReference<>();
    private final AtomicReference<TSStatus> failureStatus = new AtomicReference<>();
    
    // 新增：查询级别的 prepared statements 跟踪
    private final Map<String, PreparedStatementInfo> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = ConcurrentHashMap.newKeySet();
    
    // 新增：关联的 Session（用于延迟提交）
    private final IClientSession session;
    
    public QueryStateMachine(QueryId queryId, ExecutorService executor, IClientSession session) {
        this.queryState = new StateMachine<>(...);
        this.session = session;
    }
    
    public void addPreparedStatement(String name, PreparedStatementInfo info) {
        addedPreparedStatements.put(name, info);
        // 不直接修改 Session
    }
    
    public void removePreparedStatement(String name) {
        if (session.getPreparedStatement(name) == null) {
            throw new SemanticException("Prepared statement not found: " + name);
        }
        deallocatedPreparedStatements.add(name);
        // 不直接修改 Session
    }
    
    // 查询成功时提交变更
    public void commitPreparedStatements() {
        addedPreparedStatements.forEach(session::addPreparedStatement);
        deallocatedPreparedStatements.forEach(session::removePreparedStatement);
    }
    
    // 查询失败时回滚（自动，因为不修改 Session）
    public void rollbackPreparedStatements() {
        // 不需要做任何事情，因为 Session 没有被修改
    }
    
    // 获取本次查询的变更
    public Map<String, PreparedStatementInfo> getAddedPreparedStatements() {
        return ImmutableMap.copyOf(addedPreparedStatements);
    }
    
    public Set<String> getDeallocatedPreparedStatements() {
        return ImmutableSet.copyOf(deallocatedPreparedStatements);
    }
}
```

**修改 PrepareTask**：

```java
public class PrepareTask implements IConfigTask {
    @Override
    public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
        // 获取 QueryStateMachine（需要从 context 中获取）
        QueryStateMachine stateMachine = configTaskExecutor.getQueryStateMachine();
        
        if (stateMachine != null) {
            // 存储到 QueryStateMachine
            stateMachine.addPreparedStatement(statementName, info);
        } else {
            // 兼容性：如果没有 QueryStateMachine，直接存储到 Session
            IClientSession session = SessionManager.getInstance().getCurrSession();
            session.addPreparedStatement(statementName, info);
        }
        
        return future;
    }
}
```

**修改 ConfigExecution**：

```java
public class ConfigExecution implements IQueryExecution {
    private final QueryStateMachine stateMachine;
    
    public ConfigExecution(MPPQueryContext context, ...) {
        // 创建 QueryStateMachine
        this.stateMachine = new QueryStateMachine(
            context.getQueryId(),
            executor,
            context.getSession()
        );
    }
    
    @Override
    public ExecutionResult getStatus() {
        // 查询成功时提交变更
        if (stateMachine.getState() == QueryState.FINISHED) {
            stateMachine.commitPreparedStatements();
        }
        
        return new ExecutionResult(
            // ...
            stateMachine.getAddedPreparedStatements(),
            stateMachine.getDeallocatedPreparedStatements()
        );
    }
}
```

#### 方案 B：创建独立的 PreparedStatementStateTracker

**优点**：
- 职责更清晰
- 不影响现有的 QueryStateMachine

**实现**：

```java
public class PreparedStatementStateTracker {
    private final IClientSession session;
    private final Map<String, PreparedStatementInfo> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = ConcurrentHashMap.newKeySet();
    
    public PreparedStatementStateTracker(IClientSession session) {
        this.session = session;
    }
    
    public void addPreparedStatement(String name, PreparedStatementInfo info) {
        addedPreparedStatements.put(name, info);
    }
    
    public void removePreparedStatement(String name) {
        if (session.getPreparedStatement(name) == null) {
            throw new SemanticException("Prepared statement not found: " + name);
        }
        deallocatedPreparedStatements.add(name);
    }
    
    public void commit() {
        addedPreparedStatements.forEach(session::addPreparedStatement);
        deallocatedPreparedStatements.forEach(session::removePreparedStatement);
    }
    
    public void rollback() {
        // 自动回滚，因为 Session 没有被修改
    }
}
```

### 3.3 关键设计决策

#### 决策 1：何时提交到 Session？

**选项 A：查询成功时提交（推荐）**
```java
// 在 QueryExecution/ConfigExecution 成功完成时
if (queryState == QueryState.FINISHED) {
    stateMachine.commitPreparedStatements();
}
```

**选项 B：立即提交（当前实现）**
```java
// 在 PrepareTask 中立即提交
session.addPreparedStatement(name, info);
```

**推荐选项 A**，因为：
- 支持事务性
- 查询失败时自动回滚
- 符合 Trino 的设计理念

#### 决策 2：ConfigExecution 是否需要 QueryStateMachine？

**当前情况**：
- `ConfigExecution` 用于 DDL 和配置语句
- `QueryExecution` 用于查询语句
- `PREPARE` 和 `DEALLOCATE` 走 `ConfigExecution` 路径

**建议**：
- **是**，`ConfigExecution` 也需要 QueryStateMachine
- 因为 `PREPARE`/`DEALLOCATE` 也需要事务性支持
- 可以统一状态管理

#### 决策 3：如何获取 QueryStateMachine？

**选项 A：通过 IConfigTaskExecutor**
```java
public interface IConfigTaskExecutor {
    QueryStateMachine getQueryStateMachine();
    IClientSession getCurrSession();
}
```

**选项 B：通过 MPPQueryContext**
```java
public class MPPQueryContext {
    private QueryStateMachine queryStateMachine;
    
    public QueryStateMachine getQueryStateMachine() {
        if (queryStateMachine == null) {
            queryStateMachine = new QueryStateMachine(queryId, executor, session);
        }
        return queryStateMachine;
    }
}
```

**推荐选项 B**，因为：
- 更符合依赖注入原则
- 便于统一管理

## 4. 实施建议

### 4.1 短期实施（最小改动）

1. **扩展 QueryStateMachine**
   - 添加 `addedPreparedStatements` 和 `deallocatedPreparedStatements`
   - 添加 `commitPreparedStatements()` 方法

2. **修改 PrepareTask 和 DeallocateTask**
   - 存储到 QueryStateMachine 而不是直接存储到 Session

3. **修改 ConfigExecution**
   - 在查询成功时提交变更

### 4.2 中期优化

1. **统一 QueryExecution 和 ConfigExecution 的状态管理**
   - 两者都使用 QueryStateMachine

2. **添加查询信息记录**
   - ExecutionResult 包含 prepared statements 变更

3. **支持查询历史**
   - 记录每个查询的 prepared statements 变更

### 4.3 长期优化

1. **支持事务回滚**
   - 查询失败时自动回滚 prepared statements 变更

2. **添加审计日志**
   - 记录 prepared statements 的创建/删除历史

3. **性能优化**
   - 批量提交 prepared statements
   - 缓存常用的 prepared statements

## 5. 总结

### Trino 设计的核心优势：

1. ✅ **事务性**：查询失败时自动回滚
2. ✅ **查询级别跟踪**：每个查询的变更独立跟踪
3. ✅ **延迟提交**：只有成功时才提交到 Session
4. ✅ **查询信息完整性**：QueryInfo 包含所有变更

### IoTDB 应该采用的设计：

1. ✅ **扩展 QueryStateMachine**：添加 prepared statements 跟踪
2. ✅ **延迟提交**：查询成功时提交到 Session
3. ✅ **统一状态管理**：ConfigExecution 和 QueryExecution 都使用 QueryStateMachine
4. ✅ **保持 AST 存储优势**：继续存储 AST，而不是 SQL 字符串

### 关键区别：

| 特性 | Trino | IoTDB（推荐） |
|------|-------|---------------|
| **存储格式** | SQL 字符串 | AST（保持优势） |
| **状态跟踪** | QueryStateMachine | QueryStateMachine（扩展） |
| **提交时机** | 查询完成时 | 查询成功时 |
| **事务性** | ✅ 支持 | ✅ 支持（新增） |

通过借鉴 Trino 的设计理念，IoTDB 可以在保持自身优势（AST 存储）的同时，获得事务性和查询级别跟踪的能力。




