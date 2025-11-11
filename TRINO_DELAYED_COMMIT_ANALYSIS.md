# Trino "延迟提交和错误恢复" 设计依据分析

## 1. 关键发现

通过分析 Trino 的代码，我发现"延迟提交"的设计实际上体现在以下几个方面：

### 1.1 服务端：QueryStateMachine 不直接修改 Session

**证据 1：PrepareTask 只修改 QueryStateMachine**

```173:174:d:/TsinghuaSE/trino-master/core/trino-main/src/main/java/io/trino/execution/QueryStateMachine.java
    private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = Sets.newConcurrentHashSet();
```

```1131:1137:d:/TsinghuaSE/trino-master/core/trino-main/src/main/java/io/trino/execution/QueryStateMachine.java
    public void addPreparedStatement(String key, String value)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");

        addedPreparedStatements.put(key, value);
    }
```

**关键点**：
- `addPreparedStatement()` 只修改 `addedPreparedStatements` Map
- **没有调用 `session.addPreparedStatement()`**
- Session 的 `preparedStatements` 字段**从未被修改**

**证据 2：QueryInfo 包含 Session 快照和增量变更**

```667:686:d:/TsinghuaSE/trino-master/core/trino-main/src/main/java/io/trino/execution/QueryStateMachine.java
        return new QueryInfo(
                queryId,
                session.toSessionRepresentation(),  // Session 的快照（查询开始时的状态）
                state,
                self,
                outputManager.getQueryOutputInfo().map(QueryOutputInfo::getColumnNames).orElse(ImmutableList.of()),
                query,
                preparedQuery,
                queryStats,
                Optional.ofNullable(setCatalog.get()),
                Optional.ofNullable(setSchema.get()),
                Optional.ofNullable(setPath.get()),
                Optional.ofNullable(setAuthorizationUser.get()),
                resetAuthorizationUser.get(),
                setOriginalRoles,
                setSessionProperties,
                resetSessionProperties,
                setRoles,
                addedPreparedStatements,           // 本次查询的增量变更
                deallocatedPreparedStatements,     // 本次查询的增量变更
```

**关键点**：
- `session.toSessionRepresentation()` 是查询**开始时的 Session 快照**
- `addedPreparedStatements` 和 `deallocatedPreparedStatements` 是**本次查询的增量变更**
- QueryInfo 同时包含快照和增量，客户端可以据此更新

### 1.2 服务端：通过 HTTP 响应头发送增量变更

**证据：ExecutingStatementResource 设置响应头**

服务端通过 HTTP 响应头发送 prepared statements 的增量变更，而不是通过 QueryInfo JSON：

```java
// ExecutingStatementResource.java
for (Map.Entry<String, String> entry : resultsResponse.addedPreparedStatements().entrySet()) {
    response.header(protocolHeaders.responseAddedPrepare(), 
        urlEncode(entry.getKey()) + "=" + urlEncode(entry.getValue()));
}
for (String name : resultsResponse.deallocatedPreparedStatements()) {
    response.header(protocolHeaders.responseDeallocatedPrepare(), urlEncode(name));
}
```

**关键点**：
- 服务端**不通过 QueryInfo JSON** 发送增量变更
- 而是通过 **HTTP 响应头**（`X-Trino-Added-Prepare` 和 `X-Trino-Deallocated-Prepare`）发送
- 这样可以在查询执行过程中逐步发送增量变更，而不需要等待查询完成

### 1.3 客户端：StatementClient 从 HTTP 响应头提取增量变更

**证据：StatementClientV1.processResponse()**

`StatementClient` **没有直接封装 QueryInfo**，而是从 HTTP 响应头中提取增量变更：

```570:579:d:/TsinghuaSE/trino-master/client/trino-client/src/main/java/io/trino/client/StatementClientV1.java
        for (String entry : headers.values(TRINO_HEADERS.responseAddedPrepare())) {
            List<String> keyValue = COLLECTION_HEADER_SPLITTER.splitToList(entry);
            if (keyValue.size() != 2) {
                continue;
            }
            addedPreparedStatements.put(urlDecode(keyValue.get(0)), urlDecode(keyValue.get(1)));
        }
        for (String entry : headers.values(TRINO_HEADERS.responseDeallocatedPrepare())) {
            deallocatedPreparedStatements.add(urlDecode(entry));
        }
```

**关键点**：
- `StatementClient` **不封装 QueryInfo**，而是从 HTTP 响应头中提取增量变更
- 每次收到 HTTP 响应时，`processResponse()` 方法会解析响应头并更新 `addedPreparedStatements` 和 `deallocatedPreparedStatements`
- 这些字段是**累积的**，在整个查询执行过程中逐步收集

### 1.4 客户端：根据 StatementClient 更新本地 Session

**证据 1：JDBC 客户端 - TrinoConnection.updateSession()**

```933:943:d:/TsinghuaSE/trino-master/client/trino-jdbc/src/main/java/io/trino/jdbc/TrinoConnection.java
    void updateSession(StatementClient client)
    {
        sessionProperties.putAll(client.getSetSessionProperties());
        client.getResetSessionProperties().forEach(sessionProperties::remove);

        preparedStatements.putAll(client.getAddedPreparedStatements());
        client.getDeallocatedPreparedStatements().forEach(preparedStatements::remove);
```

**证据 2：CLI 客户端 - Console.process()**

```433:438:d:/TsinghuaSE/trino-master/client/trino-cli/src/main/java/io/trino/cli/Console.java
            // update prepared statements if present
            if (!query.getAddedPreparedStatements().isEmpty() || !query.getDeallocatedPreparedStatements().isEmpty()) {
                Map<String, String> preparedStatements = new HashMap<>(session.getPreparedStatements());
                preparedStatements.putAll(query.getAddedPreparedStatements());
                preparedStatements.keySet().removeAll(query.getDeallocatedPreparedStatements());
                builder = builder.preparedStatements(preparedStatements);
            }
```

**关键点**：
- **所有客户端**（JDBC、CLI 等）都从 `StatementClient` 获取增量变更
- `StatementClient` 提供了 `getAddedPreparedStatements()` 和 `getDeallocatedPreparedStatements()` 方法
- 客户端**自己负责**将增量变更应用到本地的 Session
- 如果查询失败，客户端可能不调用 `updateSession()`，或者即使调用了，也可以根据查询状态忽略增量变更

## 2. "延迟提交"的真正含义

### 2.1 服务端视角

**Trino 服务端的设计**：

```
PREPARE stmt1
    ↓
QueryStateMachine.addPreparedStatement("stmt1", sql)
    ├─ 只修改 QueryStateMachine.addedPreparedStatements
    └─ Session.preparedStatements 保持不变
    ↓
查询执行...
    ↓
构建 QueryInfo
    ├─ session.toSessionRepresentation() → Session 快照（查询开始时的状态）
    └─ addedPreparedStatements → 本次查询的增量变更
    ↓
返回 QueryInfo 给客户端
```

**关键点**：
- **服务端的 Session 从未被修改**
- QueryStateMachine 只跟踪增量变更
- QueryInfo 包含 Session 快照 + 增量变更

### 2.2 客户端视角

**Trino 客户端的设计**：

```
收到 QueryInfo
    ↓
检查查询状态
    ├─ 如果成功 → 调用 updateSession()
    │   └─ preparedStatements.putAll(addedPreparedStatements)
    │   └─ deallocatedPreparedStatements.forEach(preparedStatements::remove)
    └─ 如果失败 → 不更新 Session（或 QueryInfo 中增量为空）
```

**关键点**：
- **客户端负责更新本地的 Session**
- 只有查询成功时，客户端才会应用增量变更
- 查询失败时，客户端不更新，Session 保持不变

## 3. "错误恢复"的实现机制

### 3.1 查询失败时的行为

**场景**：查询执行过程中发生错误

```
PREPARE stmt1
    ↓
QueryStateMachine.addPreparedStatement("stmt1", sql)
    ├─ addedPreparedStatements.put("stmt1", sql)
    └─ Session.preparedStatements 不变
    ↓
查询执行失败
    ↓
transitionToFailed()
    ├─ QueryStateMachine 被标记为 FAILED
    └─ QueryInfo 仍然包含 addedPreparedStatements
    ↓
返回 QueryInfo 给客户端
    ├─ state = FAILED
    └─ addedPreparedStatements = {"stmt1": "SELECT ..."}
    ↓
客户端收到 QueryInfo
    ├─ 检测到 state = FAILED
    └─ 不调用 updateSession() 或忽略增量变更
    ↓
结果：客户端的 Session 不受影响
```

**关键点**：
- 服务端的 Session **从未被修改**，所以不需要回滚
- 客户端的 Session **只有在查询成功时才更新**
- 查询失败时，客户端不更新 Session，自动"回滚"

### 3.2 为什么不需要显式回滚？

**原因**：
1. **服务端 Session 从未被修改**
   - QueryStateMachine 只跟踪增量变更
   - Session 的 `preparedStatements` 字段保持不变
   - 查询失败时，QueryStateMachine 被丢弃，但 Session 不受影响

2. **客户端只在成功时更新**
   - 客户端根据查询状态决定是否更新 Session
   - 查询失败时，客户端不调用 `updateSession()`
   - 即使调用了，也可以根据 `state == FAILED` 忽略增量变更

## 4. 代码证据总结

### 4.1 服务端：QueryStateMachine 不修改 Session

**证据位置**：
- `QueryStateMachine.addPreparedStatement()` (line 1131-1137)
  - 只修改 `addedPreparedStatements` Map
  - **没有调用 `session.addPreparedStatement()`**

- `QueryStateMachine.removePreparedStatement()` (line 1139-1147)
  - 只修改 `deallocatedPreparedStatements` Set
  - **没有调用 `session.removePreparedStatement()`**
  - 只检查 Session 中是否存在（用于验证）

### 4.2 QueryInfo 包含快照和增量

**证据位置**：
- `QueryStateMachine.getQueryInfo()` (line 667-686)
  - `session.toSessionRepresentation()` - Session 快照
  - `addedPreparedStatements` - 增量变更
  - `deallocatedPreparedStatements` - 增量变更

### 4.3 服务端通过 HTTP 响应头发送增量变更

**证据位置**：
- `ExecutingStatementResource` (line 250-261)
  - 通过 `responseAddedPrepare` 和 `responseDeallocatedPrepare` HTTP 响应头发送增量变更

### 4.4 StatementClient 从 HTTP 响应头提取增量变更

**证据位置**：
- `StatementClientV1.processResponse()` (line 570-579)
  - 从 HTTP 响应头中提取 `addedPreparedStatements` 和 `deallocatedPreparedStatements`
  - 这些字段是累积的，在整个查询执行过程中逐步收集

### 4.5 客户端负责更新 Session

**证据位置**：
- **JDBC 客户端**：`TrinoConnection.updateSession()` (line 933-943)
  - `preparedStatements.putAll(client.getAddedPreparedStatements())`
  - `client.getDeallocatedPreparedStatements().forEach(preparedStatements::remove)`

- **CLI 客户端**：`Console.process()` (line 433-438)
  - `preparedStatements.putAll(query.getAddedPreparedStatements())`
  - `preparedStatements.keySet().removeAll(query.getDeallocatedPreparedStatements())`

## 5. 与 IoTDB 的对比

### 5.1 IoTDB 当前实现

```java
// PrepareTask.execute()
session.addPreparedStatement(statementName, info);  // 立即修改 Session
```

**问题**：
- 如果查询后续失败，prepared statement 已经存在于 Session 中
- 无法自动回滚
- 需要手动清理

### 5.2 Trino 的实现

```java
// PrepareTask.execute()
stateMachine.addPreparedStatement(name, sql);  // 只修改 QueryStateMachine

// 查询成功时
QueryInfo queryInfo = stateMachine.getQueryInfo();
// QueryInfo 包含 addedPreparedStatements

// 客户端
if (queryInfo.getState() == FINISHED) {
    connection.updateSession(queryInfo);  // 客户端更新本地 Session
}
```

**优势**：
- 服务端 Session 从未被修改
- 查询失败时自动"回滚"（因为 Session 从未被修改）
- 客户端只在成功时更新

## 6. 重要澄清

### 6.1 "延迟提交"的真正含义

**不是**：服务端延迟提交到 Session
**而是**：
1. **服务端**：QueryStateMachine 跟踪增量变更，不修改 Session
2. **客户端**：根据 QueryInfo 中的增量变更，在查询成功时更新本地 Session

### 6.2 "错误恢复"的实现

**不是**：显式回滚操作
**而是**：
1. **服务端**：Session 从未被修改，所以不需要回滚
2. **客户端**：查询失败时不更新 Session，自动"回滚"

### 6.3 为什么这样设计？

**原因**：
1. **服务端 Session 是只读的**（在查询执行期间）
   - Session 在查询开始时创建快照
   - 查询执行期间，Session 保持不变
   - 查询完成后，客户端根据 QueryInfo 更新

2. **支持客户端状态同步**
   - 客户端需要知道查询的变更
   - QueryInfo 提供完整的变更信息
   - 客户端可以根据需要更新本地状态

3. **简化错误处理**
   - 查询失败时，服务端 Session 不受影响
   - 客户端不更新，自动"回滚"
   - 不需要显式的回滚逻辑

## 7. Trino 的完整数据流详解

### 7.0 重要澄清：Trino 使用 HTTP/REST API，不是 Thrift

**证据 1：Trino 客户端使用 HTTP**

```27:34:d:/TsinghuaSE/trino-master/client/trino-client/src/main/java/io/trino/client/StatementClientV1.java
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
```

```83:83:d:/TsinghuaSE/trino-master/client/trino-client/src/main/java/io/trino/client/StatementClientV1.java
    private final Call.Factory httpCallFactory;
```

```148:157:d:/TsinghuaSE/trino-master/client/trino-client/src/main/java/io/trino/client/StatementClientV1.java
    private Request buildQueryRequest(ClientSession session, String query, Optional<String> requestedEncoding)
    {
        HttpUrl url = HttpUrl.get(session.getServer());
        if (url == null) {
            throw new ClientException("Invalid server URL: " + session.getServer());
        }
        url = url.newBuilder().encodedPath("/v1/statement").build();

        Request.Builder builder = prepareRequest(url)
                .post(RequestBody.create(query, MEDIA_TYPE_TEXT));
```

```493:520:d:/TsinghuaSE/trino-master/client/trino-client/src/main/java/io/trino/client/StatementClientV1.java
            JsonResponse<QueryResults> response;
            try {
                response = JsonResponse.execute(QUERY_RESULTS_CODEC, httpCallFactory, request);
                nextHeartbeat.set(System.nanoTime() + heartbeatInterval);
            }
            catch (RuntimeException e) {
                if (!isRetryable.apply(e)) {
                    throw e;
                }
                cause = e;
                continue;
            }
            if (isTransient(response.getException())) {
                cause = response.getException();
                continue;
            }
            if (response.getStatusCode() != HTTP_OK || !response.hasValue()) {
                if (!shouldRetry(response.getStatusCode())) {
                    state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
                    throw requestFailedException(taskName, request, response);
                }
                cause = new ClientException(format("Expected http code %d but got %d%s", HTTP_OK, response.getStatusCode(), response.getResponseBody()
                                .map(message -> "\nResponse body was: " + message)
                                .orElse("")));
                continue;
            }

            processResponse(response.getHeaders(), response.getValue());
```

**证据 2：Trino 服务端使用 REST API（JAX-RS）**

```34:46:d:/TsinghuaSE/trino-master/core/trino-main/src/main/java/io/trino/server/protocol/ExecutingStatementResource.java
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;
```

```67:69:d:/TsinghuaSE/trino-master/core/trino-main/src/main/java/io/trino/server/protocol/ExecutingStatementResource.java
@Path("/v1/statement/executing")
@ResourceSecurity(PUBLIC)
public class ExecutingStatementResource
```

```145:157:d:/TsinghuaSE/trino-master/core/trino-main/src/main/java/io/trino/server/protocol/ExecutingStatementResource.java
    @GET
    @Path("{queryId}/{slug}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getQueryResults(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token,
            @BeanParam ExternalUriInfo externalUriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        Query query = getQuery(queryId, slug, token);
        asyncQueryResults(query, token, externalUriInfo, asyncResponse);
    }
```

**对比：IoTDB 使用 Thrift RPC**

IoTDB 的客户端使用 Thrift：
- `IClientRPCService.Client`（Thrift 客户端）
- `TTransport`、`TProtocol`（Thrift 传输和协议）
- `TSocket`（Thrift Socket）

**总结**：
- ✅ **Trino 使用 HTTP/REST API**（OkHttp + JAX-RS）
- ✅ **IoTDB 使用 Thrift RPC**（TProtocol + TSocket）
- ✅ 这是两个系统在通信协议上的**根本区别**

### 7.1 简化流程图

```
PrepareTask.execute()
    │
    └─> QueryStateMachine.addPreparedStatement("stmt1", sql)
            │
            ├─> QueryStateMachine.addedPreparedStatements.put("stmt1", sql)
            └─> Session.preparedStatements (保持不变)
                    │
                    ↓
QueryManager.getResultQueryInfo(queryId)
    │
    └─> QueryStateMachine.getResultQueryInfo(stages)
            │
            └─> new ResultQueryInfo(..., addedPreparedStatements, ...)
                    │
                    ↓
Query.getNextResult(token)
    │
    ├─> ResultQueryInfo queryInfo = queryManager.getResultQueryInfo(queryId)
    ├─> addedPreparedStatements = queryInfo.addedPreparedStatements()
    ├─> QueryResults queryResults = new QueryResults(...)
    └─> toResultsResponse(queryResults)
            │
            └─> new QueryResultsResponse(..., addedPreparedStatements, queryResults, ...)
                    │
                    ↓
ExecutingStatementResource.toResponse(resultsResponse)
    │
    ├─> Response.ok(resultsResponse.queryResults())  // 响应体：QueryResults JSON
    └─> response.header("X-Trino-Added-Prepare", "stmt1=SELECT...")  // 响应头：增量变更
            │
            ↓
HTTP 响应
    ├─ Headers: X-Trino-Added-Prepare: stmt1=SELECT...
    └─ Body: {"id": "...", "columns": [...], "data": [...]}
            │
            ↓
StatementClientV1.processResponse(headers, results)
    │
    ├─> headers.values("X-Trino-Added-Prepare")
    │   └─> this.addedPreparedStatements.put(name, sql)  // 累积
    └─> headers.values("X-Trino-Deallocated-Prepare")
        └─> this.deallocatedPreparedStatements.add(name)  // 累积
                │
                ↓
客户端 updateSession(client)
    │
    ├─> client.getAddedPreparedStatements()
    │   └─> preparedStatements.putAll(...)
    └─> client.getDeallocatedPreparedStatements()
        └─> preparedStatements.remove(...)
```

### 7.2 完整数据流：从 QueryStateMachine 到客户端 Session

```
┌─────────────────────────────────────────────────────────────────┐
│ 步骤 1: QueryStateMachine (查询级临时存储)                        │
└─────────────────────────────────────────────────────────────────┘
    │
    │ PrepareTask.execute()
    │   stateMachine.addPreparedStatement("stmt1", sql)
    │
    ├─ addedPreparedStatements: Map<String, String>
    │   └─ {"stmt1": "SELECT * FROM table WHERE id = ?"}
    │
    └─ deallocatedPreparedStatements: Set<String>
        └─ {"stmt2"}

    ↓

┌─────────────────────────────────────────────────────────────────┐
│ 步骤 2: QueryStateMachine.getResultQueryInfo()                  │
│ 位置: QueryStateMachine.java:547                                │
└─────────────────────────────────────────────────────────────────┘
    │
    │ ResultQueryInfo queryInfo = stateMachine.getResultQueryInfo(stages)
    │
    ├─ 从 QueryStateMachine 提取增量变更
    │   ├─ addedPreparedStatements
    │   └─ deallocatedPreparedStatements
    │
    └─ 构建 ResultQueryInfo（包含增量变更）

    ↓

┌─────────────────────────────────────────────────────────────────┐
│ 步骤 3: Query.getNextResult()                                   │
│ 位置: Query.java:425                                            │
└─────────────────────────────────────────────────────────────────┘
    │
    │ ResultQueryInfo queryInfo = queryManager.getResultQueryInfo(queryId)
    │
    ├─ 从 ResultQueryInfo 提取增量变更
    │   ├─ addedPreparedStatements = queryInfo.addedPreparedStatements()
    │   └─ deallocatedPreparedStatements = queryInfo.deallocatedPreparedStatements()
    │
    ├─ 构建 QueryResults（查询结果数据）
    │   └─ new QueryResults(queryId, columns, queryData, stats, ...)
    │
    └─ 调用 toResultsResponse(queryResults)

    ↓

┌─────────────────────────────────────────────────────────────────┐
│ 步骤 4: Query.toResultsResponse()                               │
│ 位置: Query.java:545                                            │
└─────────────────────────────────────────────────────────────────┘
    │
    │ QueryResultsResponse response = new QueryResultsResponse(
    │     setCatalog, setSchema, setPath,
    │     addedPreparedStatements,      // 增量变更
    │     deallocatedPreparedStatements, // 增量变更
    │     queryResults,                  // QueryResults JSON
    │     ...
    │ )
    │
    └─ 包装 QueryResults 和增量变更

    ↓

┌─────────────────────────────────────────────────────────────────┐
│ 步骤 5: ExecutingStatementResource.toResponse()                 │
│ 位置: ExecutingStatementResource.java:222                       │
└─────────────────────────────────────────────────────────────────┘
    │
    │ Response response = Response.ok(resultsResponse.queryResults())
    │
    ├─ 设置 HTTP 响应体（QueryResults JSON）
    │   └─ response.entity(resultsResponse.queryResults())
    │
    └─ 设置 HTTP 响应头（增量变更）
        ├─ response.header("X-Trino-Added-Prepare", "stmt1=SELECT...")
        └─ response.header("X-Trino-Deallocated-Prepare", "stmt2")

    ↓
    │
    │ HTTP 响应
    │ ├─ Headers:
    │ │   ├─ X-Trino-Added-Prepare: stmt1=SELECT%20*%20FROM...
    │ │   └─ X-Trino-Deallocated-Prepare: stmt2
    │ └─ Body (JSON):
    │     {
    │       "id": "20240101_120000_00001_abcde",
    │       "columns": [...],
    │       "data": [...],
    │       "stats": {...}
    │     }
    │
    ↓

┌─────────────────────────────────────────────────────────────────┐
│ 步骤 6: StatementClientV1.processResponse()                     │
│ 位置: StatementClientV1.java:533                                │
└─────────────────────────────────────────────────────────────────┘
    │
    │ processResponse(Headers headers, QueryResults results)
    │
    ├─ 从 HTTP 响应头提取增量变更
    │   ├─ for (String entry : headers.values("X-Trino-Added-Prepare"))
    │   │   └─ addedPreparedStatements.put(name, sql)
    │   │
    │   └─ for (String entry : headers.values("X-Trino-Deallocated-Prepare"))
    │       └─ deallocatedPreparedStatements.add(name)
    │
    └─ 累积到 StatementClient 的字段中
        ├─ this.addedPreparedStatements (累积)
        └─ this.deallocatedPreparedStatements (累积)

    ↓

┌─────────────────────────────────────────────────────────────────┐
│ 步骤 7: 客户端更新本地 Session                                  │
└─────────────────────────────────────────────────────────────────┘
    │
    │ JDBC 客户端: TrinoConnection.updateSession(client)
    │ CLI 客户端: Console.process(query)
    │
    ├─ 从 StatementClient 获取增量变更
    │   ├─ client.getAddedPreparedStatements()
    │   └─ client.getDeallocatedPreparedStatements()
    │
    └─ 更新本地 Session
        ├─ preparedStatements.putAll(addedPreparedStatements)
        └─ deallocatedPreparedStatements.forEach(preparedStatements::remove)
```

### 7.3 关键代码位置

#### 步骤 1: QueryStateMachine 存储增量变更

```1131:1147:d:/TsinghuaSE/trino-master/core/trino-main/src/main/java/io/trino/execution/QueryStateMachine.java
    public void addPreparedStatement(String key, String value)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");

        addedPreparedStatements.put(key, value);
    }

    public void removePreparedStatement(String key)
    {
        requireNonNull(key, "key is null");

        if (!session.getPreparedStatements().containsKey(key)) {
            throw new TrinoException(NOT_FOUND, "Prepared statement not found: " + key);
        }
        deallocatedPreparedStatements.add(key);
    }
```

#### 步骤 2: QueryStateMachine.getResultQueryInfo()

```547:597:d:/TsinghuaSE/trino-master/core/trino-main/src/main/java/io/trino/execution/QueryStateMachine.java
    ResultQueryInfo getResultQueryInfo(Optional<BasicStagesInfo> stagesInfo)
    {
        QueryState state = queryState.get();
        // ...
        return new ResultQueryInfo(
            queryId,
            state,
            // ...
            addedPreparedStatements,           // 从 QueryStateMachine 提取
            deallocatedPreparedStatements,     // 从 QueryStateMachine 提取
            // ...
        );
    }
```

#### 步骤 3: Query.getNextResult()

```425:542:d:/TsinghuaSE/trino-master/core/trino-main/src/main/java/io/trino/server/protocol/Query.java
    private synchronized QueryResultsResponse getNextResult(long token, ExternalUriInfo externalUriInfo)
    {
        // 从 QueryManager 获取 ResultQueryInfo
        ResultQueryInfo queryInfo = queryManager.getResultQueryInfo(queryId);
        
        // 提取增量变更
        addedPreparedStatements = queryInfo.addedPreparedStatements();
        deallocatedPreparedStatements = queryInfo.deallocatedPreparedStatements();
        
        // 构建 QueryResults
        QueryResults queryResults = new QueryResults(
            queryId.id(),
            // ... 查询结果数据
        );
        
        return toResultsResponse(queryResults);
    }
```

#### 步骤 4: Query.toResultsResponse()

```545:564:d:/TsinghuaSE/trino-master/core/trino-main/src/main/java/io/trino/server/protocol/Query.java
    private synchronized QueryResultsResponse toResultsResponse(QueryResults queryResults)
    {
        return new QueryResultsResponse(
                setCatalog,
                setSchema,
                setPath,
                // ...
                addedPreparedStatements,           // 增量变更
                deallocatedPreparedStatements,     // 增量变更
                // ...
                queryResults,                      // QueryResults JSON
                // ...
        );
    }
```

#### 步骤 5: ExecutingStatementResource.toResponse()

```222:280:d:/TsinghuaSE/trino-master/core/trino-main/src/main/java/io/trino/server/protocol/ExecutingStatementResource.java
    private Response toResponse(QueryResultsResponse resultsResponse)
    {
        ResponseBuilder response = Response.ok(resultsResponse.queryResults());
        
        // 设置 HTTP 响应头（增量变更）
        for (Entry<String, String> entry : resultsResponse.addedPreparedStatements().entrySet()) {
            response.header(protocolHeaders.responseAddedPrepare(), 
                urlEncode(entry.getKey()) + "=" + urlEncode(entry.getValue()));
        }
        for (String name : resultsResponse.deallocatedPreparedStatements()) {
            response.header(protocolHeaders.responseDeallocatedPrepare(), urlEncode(name));
        }
        
        return response.build();
    }
```

#### 步骤 6: StatementClientV1.processResponse()

```570:579:d:/TsinghuaSE/trino-master/client/trino-client/src/main/java/io/trino/client/StatementClientV1.java
        for (String entry : headers.values(TRINO_HEADERS.responseAddedPrepare())) {
            List<String> keyValue = COLLECTION_HEADER_SPLITTER.splitToList(entry);
            if (keyValue.size() != 2) {
                continue;
            }
            addedPreparedStatements.put(urlDecode(keyValue.get(0)), urlDecode(keyValue.get(1)));
        }
        for (String entry : headers.values(TRINO_HEADERS.responseDeallocatedPrepare())) {
            deallocatedPreparedStatements.add(urlDecode(entry));
        }
```

#### 步骤 7: 客户端更新 Session

**JDBC 客户端**：
```933:943:d:/TsinghuaSE/trino-master/client/trino-jdbc/src/main/java/io/trino/jdbc/TrinoConnection.java
    void updateSession(StatementClient client)
    {
        preparedStatements.putAll(client.getAddedPreparedStatements());
        client.getDeallocatedPreparedStatements().forEach(preparedStatements::remove);
    }
```

**CLI 客户端**：
```433:438:d:/TsinghuaSE/trino-master/client/trino-cli/src/main/java/io/trino/cli/Console.java
            if (!query.getAddedPreparedStatements().isEmpty() || !query.getDeallocatedPreparedStatements().isEmpty()) {
                Map<String, String> preparedStatements = new HashMap<>(session.getPreparedStatements());
                preparedStatements.putAll(query.getAddedPreparedStatements());
                preparedStatements.keySet().removeAll(query.getDeallocatedPreparedStatements());
                builder = builder.preparedStatements(preparedStatements);
            }
```

### 7.4 每个步骤的关键点

#### 步骤 1: QueryStateMachine
- **作用**：查询级临时存储，跟踪本次查询的增量变更
- **关键**：不修改 Session，只存储增量变更
- **数据**：`addedPreparedStatements` 和 `deallocatedPreparedStatements`

#### 步骤 2: QueryStateMachine.getResultQueryInfo()
- **作用**：从 QueryStateMachine 提取增量变更，构建 ResultQueryInfo
- **关键**：ResultQueryInfo 是 QueryInfo 的简化版本，用于查询结果响应
- **数据流**：QueryStateMachine → ResultQueryInfo

#### 步骤 3: Query.getNextResult()
- **作用**：从 QueryManager 获取 ResultQueryInfo，提取增量变更，构建 QueryResults
- **关键**：
  - QueryResults 包含查询结果数据（JSON）
  - 增量变更存储在 Query 对象的字段中（不是 QueryResults）
- **数据流**：ResultQueryInfo → QueryResults + 增量变更字段

#### 步骤 4: Query.toResultsResponse()
- **作用**：将 QueryResults 和增量变更包装成 QueryResultsResponse
- **关键**：QueryResultsResponse 是一个包装类，包含 QueryResults 和所有增量变更
- **数据流**：QueryResults + 增量变更 → QueryResultsResponse

#### 步骤 5: ExecutingStatementResource.toResponse()
- **作用**：将 QueryResultsResponse 转换为 HTTP 响应
- **关键**：
  - **QueryResults 放在 HTTP 响应体**（JSON）
  - **增量变更放在 HTTP 响应头**（`X-Trino-Added-Prepare` 等）
- **原因**：这样可以在查询执行过程中逐步发送增量变更，而不需要等待查询完成

#### 步骤 6: StatementClientV1.processResponse()
- **作用**：从 HTTP 响应头提取增量变更，累积到 StatementClient 的字段中
- **关键**：
  - 每次收到 HTTP 响应时都会调用 `processResponse()`
  - 增量变更是**累积的**，在整个查询执行过程中逐步收集
- **数据流**：HTTP 响应头 → StatementClient 字段

#### 步骤 7: 客户端更新 Session
- **作用**：客户端从 StatementClient 获取增量变更，更新本地 Session
- **关键**：
  - 客户端**自己负责**更新本地 Session
  - 只有查询成功时，客户端才会应用增量变更
  - 查询失败时，客户端不更新，自动"回滚"

### 7.5 QueryInfo vs ResultQueryInfo vs QueryResults

**重要澄清**：这三个类的作用不同：

1. **QueryInfo**：
   - **用途**：完整的查询信息，用于查询历史记录和监控
   - **包含**：Session 快照 + 增量变更 + 完整的执行计划 + 详细的统计信息
   - **位置**：`QueryStateMachine.getQueryInfo()` (line 646)
   - **使用场景**：查询历史、监控、调试

2. **ResultQueryInfo**：
   - **用途**：QueryInfo 的简化版本，用于查询结果响应
   - **包含**：增量变更 + 基本统计信息（不包含完整的执行计划）
   - **位置**：`QueryStateMachine.getResultQueryInfo()` (line 547)
   - **使用场景**：HTTP 查询结果响应

3. **QueryResults**：
   - **用途**：查询结果数据（JSON 格式）
   - **包含**：查询 ID、列信息、数据行、统计信息
   - **位置**：`Query.getNextResult()` 中构建 (line 525)
   - **使用场景**：HTTP 响应体（JSON）

**关键点**：
- **增量变更不放在 QueryResults JSON 中**，而是通过 HTTP 响应头发送
- **ResultQueryInfo 用于提取增量变更**，然后传递给 QueryResultsResponse
- **QueryResultsResponse 包含 QueryResults 和增量变更**，分别放在响应体和响应头

### 7.6 关键设计点总结

1. **服务端**：
   - QueryStateMachine 跟踪增量变更，不修改 Session
   - 通过 HTTP 响应头发送增量变更（不是 QueryInfo JSON）
   - 可以在查询执行过程中逐步发送

2. **数据转换链**：
   - QueryStateMachine → ResultQueryInfo → QueryResultsResponse → HTTP 响应
   - 增量变更在整个链中传递，最终通过 HTTP 响应头发送

3. **StatementClient**：
   - 不封装 QueryInfo，而是从 HTTP 响应头提取增量变更
   - 累积整个查询执行过程中的所有增量变更
   - 提供 `getAddedPreparedStatements()` 和 `getDeallocatedPreparedStatements()` 方法

4. **客户端**：
   - 从 StatementClient 获取增量变更
   - 根据查询状态决定是否更新本地 Session
   - 查询成功时更新，查询失败时不更新（自动"回滚"）

## 8. IoTDB 应该如何设计？

### 8.1 方案 A：完全模仿 Trino（推荐）

**服务端**：
```java
// QueryStateMachine 跟踪增量变更，不修改 Session
public void addPreparedStatement(String name, PreparedStatementInfo info) {
    addedPreparedStatements.put(name, info);
    // 不修改 Session
}

// 通过响应（HTTP 响应头或 RPC 响应字段）发送增量变更
public ExecutionResult getStatus() {
    return new ExecutionResult(
        // ...
        addedPreparedStatements,            // 增量变更
        deallocatedPreparedStatements       // 增量变更
    );
}
```

**客户端**：
```java
// 客户端根据 ExecutionResult 更新本地 Session
if (executionResult.getState() == FINISHED) {
    executionResult.getAddedPreparedStatements()
        .forEach(session::addPreparedStatement);
    executionResult.getDeallocatedPreparedStatements()
        .forEach(session::removePreparedStatement);
}
```

**注意**：IoTDB 使用 **Thrift RPC**，而 Trino 使用 **HTTP/REST API**。这是两个系统在通信协议上的根本区别：

| 特性 | Trino | IoTDB |
|------|-------|-------|
| **通信协议** | HTTP/REST API | Thrift RPC |
| **客户端库** | OkHttp | Thrift Client |
| **服务端框架** | JAX-RS (REST) | Thrift Server |
| **增量变更传输** | HTTP 响应头 | RPC 响应字段 |
| **请求格式** | HTTP POST + JSON | Thrift 二进制协议 |
| **响应格式** | HTTP Response + JSON | Thrift 响应对象 |

因此，IoTDB 需要通过 **RPC 响应字段**发送增量变更，而不是 HTTP 响应头。例如，可以在 `TSExecuteStatementResp` 或 `TSFetchResultsResp` 中添加 `addedPreparedStatements` 和 `deallocatedPreparedStatements` 字段。

### 8.2 方案 B：服务端延迟提交（简化版）

**如果 IoTDB 是服务端状态管理**（客户端不维护 Session 状态）：

```java
// QueryStateMachine 跟踪增量变更
public void addPreparedStatement(String name, PreparedStatementInfo info) {
    addedPreparedStatements.put(name, info);
    // 不修改 Session
}

// 查询成功时提交到 Session
public void commitPreparedStatements() {
    if (queryState.get() == QueryState.FINISHED) {
        addedPreparedStatements.forEach(session::addPreparedStatement);
        deallocatedPreparedStatements.forEach(session::removePreparedStatement);
    }
}

// 查询失败时自动回滚（不需要做任何事情）
```

**关键点**：
- 查询成功时，在 `QueryExecution` 或 `ConfigExecution` 完成时调用 `commitPreparedStatements()`
- 查询失败时，QueryStateMachine 被丢弃，Session 不受影响

## 9. 重要澄清

### 9.1 StatementClient 和 QueryInfo 的关系

**问题**：StatementClient 里面好像没有封装 QueryInfo？

**答案**：
- ✅ **StatementClient 确实没有直接封装 QueryInfo**
- ✅ **StatementClient 从 HTTP 响应头中提取增量变更**
- ✅ **QueryInfo 主要用于查询历史记录和监控，不用于客户端状态同步**

**数据流**：
```
QueryStateMachine
    ↓ (增量变更)
HTTP 响应头 (X-Trino-Added-Prepare, X-Trino-Deallocated-Prepare)
    ↓ (提取)
StatementClient.addedPreparedStatements / deallocatedPreparedStatements
    ↓ (获取)
客户端 Session
```

### 9.2 不同客户端如何处理

**问题**：updateSession 代码是在 JDBC 里面，那其他方式连接的客户端怎么办呢？

**答案**：
- ✅ **所有客户端都使用 StatementClient**
- ✅ **StatementClient 提供了统一的接口**：`getAddedPreparedStatements()` 和 `getDeallocatedPreparedStatements()`
- ✅ **不同客户端有不同的更新逻辑**：
  - **JDBC**：`TrinoConnection.updateSession()` - 直接更新 Connection 的 Session
  - **CLI**：`Console.process()` - 更新 QueryRunner 的 Session
  - **其他客户端**：类似地，从 StatementClient 获取增量变更并更新本地 Session

**关键点**：
- StatementClient 是**客户端库的核心**，所有客户端都使用它
- StatementClient 从 HTTP 响应头中提取增量变更，**与客户端类型无关**
- 每个客户端根据自己的需求更新本地 Session

## 10. 总结

### Trino 的设计核心：

1. ✅ **QueryStateMachine 不修改 Session**
   - 只跟踪增量变更
   - Session 在查询执行期间保持不变

2. ✅ **QueryInfo 包含快照和增量**
   - `session.toSessionRepresentation()` - Session 快照
   - `addedPreparedStatements` - 增量变更
   - `deallocatedPreparedStatements` - 增量变更

3. ✅ **客户端负责更新**
   - 查询成功时，客户端根据 QueryInfo 更新本地 Session
   - 查询失败时，客户端不更新，自动"回滚"

### IoTDB 应该采用的设计：

**如果 IoTDB 是服务端状态管理**（推荐）：
- QueryStateMachine 跟踪增量变更，不修改 Session
- 查询成功时，提交到 Session
- 查询失败时，自动回滚（因为 Session 从未被修改）

**关键代码位置**：
- `QueryStateMachine.addPreparedStatement()` - 只修改 QueryStateMachine
- `QueryStateMachine.getQueryInfo()` - 包含 Session 快照和增量变更
- `TrinoConnection.updateSession()` - 客户端更新本地 Session

