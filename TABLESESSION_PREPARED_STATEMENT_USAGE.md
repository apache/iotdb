# TableSession PreparedStatement 扩展说明

## 概述

为 `ITableSession` 接口和 `TableSession` 实现类添加了 PreparedStatement 支持，允许客户端预编译 SQL 语句并多次执行，提高性能和安全性。

## 新增接口方法

### 1. `prepare(String statementName, String sql)`

准备一个 SQL 语句，存储在服务器端供后续执行。

**参数**：
- `statementName`: PreparedStatement 的名称（必须是有效的标识符）
- `sql`: 要准备的 SQL 语句（可以包含 `?` 占位符）

**示例**：
```java
tableSession.prepare("stmt1", "SELECT * FROM t WHERE a = ? AND b = ?");
tableSession.prepare("insertStmt", "INSERT INTO t VALUES (?, ?, ?)");
```

### 2. `execute(String statementName, Object... parameters)`

执行一个已准备的语句，使用给定的参数。

**参数**：
- `statementName`: 要执行的 PreparedStatement 名称
- `parameters`: 参数值（按 `?` 占位符的出现顺序）

**返回值**：
- 如果语句是查询语句，返回 `SessionDataSet`
- 如果语句是非查询语句，返回 `null`

**示例**：
```java
// 执行查询
SessionDataSet result = tableSession.execute("stmt1", 100, "test");

// 执行插入（非查询）
tableSession.execute("insertStmt", 1000L, 10.5, "value");
```

### 3. `execute(String statementName, long timeoutInMs, Object... parameters)`

执行一个已准备的语句，使用给定的参数和超时时间。

**参数**：
- `statementName`: 要执行的 PreparedStatement 名称
- `timeoutInMs`: 查询超时时间（毫秒）
- `parameters`: 参数值（按 `?` 占位符的出现顺序）

**返回值**：
- 如果语句是查询语句，返回 `SessionDataSet`
- 如果语句是非查询语句，返回 `null`

**示例**：
```java
SessionDataSet result = tableSession.execute("stmt1", 5000L, 100, "test");
```

### 4. `executeImmediate(String sql, Object... parameters)`

立即执行一个 SQL 语句（不存储 PreparedStatement），使用给定的参数。

**参数**：
- `sql`: 要执行的 SQL 语句（可以包含 `?` 占位符）
- `parameters`: 参数值（按 `?` 占位符的出现顺序）

**返回值**：
- 如果语句是查询语句，返回 `SessionDataSet`
- 如果语句是非查询语句，返回 `null`

**示例**：
```java
SessionDataSet result = tableSession.executeImmediate(
    "SELECT * FROM t WHERE a = ?", 100);
```

### 5. `executeImmediate(String sql, long timeoutInMs, Object... parameters)`

立即执行一个 SQL 语句，使用给定的参数和超时时间。

**参数**：
- `sql`: 要执行的 SQL 语句（可以包含 `?` 占位符）
- `timeoutInMs`: 查询超时时间（毫秒）
- `parameters`: 参数值（按 `?` 占位符的出现顺序）

**返回值**：
- 如果语句是查询语句，返回 `SessionDataSet`
- 如果语句是非查询语句，返回 `null`

**示例**：
```java
SessionDataSet result = tableSession.executeImmediate(
    "SELECT * FROM t WHERE a = ?", 5000L, 100);
```

### 6. `deallocate(String statementName)`

释放（删除）一个已准备的语句。

**参数**：
- `statementName`: 要释放的 PreparedStatement 名称

**示例**：
```java
tableSession.deallocate("stmt1");
```

## 参数类型支持

`ParameterFormatter` 类支持以下参数类型：

| Java 类型 | SQL 字面量格式 | 示例 |
|-----------|---------------|------|
| `String` | 单引号字符串（自动转义） | `'value'` |
| `Number` (Integer, Long, Double, Float, etc.) | 直接使用 | `100`, `10.5` |
| `Boolean` | `TRUE` / `FALSE` | `TRUE` |
| `null` | `NULL` | `NULL` |
| `Timestamp` | ISO 格式时间戳字符串 | `'2024-01-01T00:00:00Z'` |
| `LocalDateTime` | ISO 格式日期时间字符串 | `'2024-01-01T00:00:00'` |
| `LocalDate` | ISO 格式日期字符串 | `'2024-01-01'` |
| 其他类型 | 转换为字符串并引用 | `'toString()'` |

## 完整使用示例

```java
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSession;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

// 创建 Session 和 TableSession
Session session = new Session("127.0.0.1", 6667, "root", "root");
session.open();
ITableSession tableSession = new TableSession(session);

try {
    // 1. 准备语句
    tableSession.prepare("queryStmt", 
        "SELECT * FROM t WHERE id = ? AND status = ?");
    
    // 2. 多次执行（使用不同参数）
    SessionDataSet result1 = tableSession.execute("queryStmt", 1, "active");
    // 处理 result1...
    
    SessionDataSet result2 = tableSession.execute("queryStmt", 2, "inactive");
    // 处理 result2...
    
    // 3. 执行插入语句
    tableSession.prepare("insertStmt", 
        "INSERT INTO t VALUES (?, ?, ?)");
    tableSession.execute("insertStmt", 1000L, 10.5, "value1");
    tableSession.execute("insertStmt", 2000L, 20.5, "value2");
    
    // 4. 使用 EXECUTE IMMEDIATE（不需要 PREPARE）
    SessionDataSet result3 = tableSession.executeImmediate(
        "SELECT COUNT(*) FROM t WHERE id > ?", 100);
    // 处理 result3...
    
    // 5. 释放 PreparedStatement
    tableSession.deallocate("queryStmt");
    tableSession.deallocate("insertStmt");
    
} catch (StatementExecutionException | IoTDBConnectionException e) {
    e.printStackTrace();
} finally {
    tableSession.close();
}
```

## 实现细节

### SQL 语句构建

1. **PREPARE 语句**：
   ```sql
   PREPARE statementName FROM 'sql'
   ```
   SQL 中的单引号会被转义为 `''`。

2. **EXECUTE 语句**：
   ```sql
   EXECUTE statementName USING param1, param2, ...
   ```
   参数会被格式化为 SQL 字面量。

3. **EXECUTE IMMEDIATE 语句**：
   ```sql
   EXECUTE IMMEDIATE 'sql' USING param1, param2, ...
   ```

4. **DEALLOCATE 语句**：
   ```sql
   DEALLOCATE PREPARE statementName
   ```

### 查询 vs 非查询处理

`execute()` 和 `executeImmediate()` 方法会：
1. 首先尝试作为查询语句执行（调用 `executeQueryStatement()`）
2. 如果失败（抛出 `StatementExecutionException`），则作为非查询语句执行（调用 `executeNonQueryStatement()`）
3. 对于非查询语句，返回 `null`

**注意**：这种方法依赖于服务器端的行为。如果服务器能够区分查询和非查询语句，这种方法可以正常工作。如果服务器对所有语句都返回结果集，可能需要调整实现。

## 错误处理

### 常见异常

1. **`StatementExecutionException`**：
   - PreparedStatement 名称已存在（PREPARE）
   - PreparedStatement 不存在（EXECUTE）
   - 参数数量不匹配
   - 参数类型不匹配
   - SQL 语法错误

2. **`IoTDBConnectionException`**：
   - 连接问题
   - 网络错误

### 错误处理示例

```java
try {
    tableSession.prepare("stmt1", "SELECT * FROM t WHERE id = ?");
    SessionDataSet result = tableSession.execute("stmt1", 100);
    // 处理结果...
} catch (StatementExecutionException e) {
    if (e.getMessage().contains("already exists")) {
        // PreparedStatement 已存在
        System.err.println("PreparedStatement already exists");
    } else if (e.getMessage().contains("not found")) {
        // PreparedStatement 不存在
        System.err.println("PreparedStatement not found");
    } else {
        // 其他执行错误
        e.printStackTrace();
    }
} catch (IoTDBConnectionException e) {
    // 连接错误
    System.err.println("Connection error: " + e.getMessage());
}
```

## 注意事项

1. **PreparedStatement 生命周期**：
   - PreparedStatement 存储在服务器端的会话中
   - 会话关闭时，所有 PreparedStatement 会被自动释放
   - 可以手动调用 `deallocate()` 释放 PreparedStatement

2. **参数顺序**：
   - 参数必须按照 SQL 中 `?` 占位符的出现顺序提供
   - 参数数量必须与占位符数量匹配

3. **SQL 注入防护**：
   - 使用 PreparedStatement 可以有效防止 SQL 注入攻击
   - 参数值会被正确转义和格式化

4. **性能考虑**：
   - `PREPARE` 语句只需要执行一次，后续 `EXECUTE` 可以跳过解析阶段
   - 对于需要多次执行的相同 SQL，使用 PreparedStatement 可以提高性能
   - 对于只执行一次的 SQL，使用 `executeImmediate()` 更简单

5. **线程安全**：
   - `TableSession` 不是线程安全的
   - 多线程环境下需要使用同步机制或为每个线程创建独立的 Session

## 与服务器端的关系

这些客户端方法最终会构建相应的 SQL 语句并发送到服务器：
- `prepare()` → `PREPARE statementName FROM 'sql'`
- `execute()` → `EXECUTE statementName USING ...`
- `executeImmediate()` → `EXECUTE IMMEDIATE 'sql' USING ...`
- `deallocate()` → `DEALLOCATE PREPARE statementName`

服务器端会处理这些语句，执行相应的 PreparedStatement 管理操作。




