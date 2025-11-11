# Prepared Statement 实现总结

## 已完成的修改

### 1. 移除 PreparedStatementInfo 中的 zoneId ✅

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/session/PreparedStatementInfo.java`

**修改内容**：
- ❌ 移除了 `zoneId` 字段
- ✅ 保留了 `statementName`、`sql`（AST）、`createTime`
- ✅ 更新了构造函数、equals、hashCode、toString 方法

**原因**：
- 执行时应使用当前会话的 `zoneId`，而不是 PREPARE 时的 `zoneId`
- AST 中的时间字面量在解析时已经固定
- 参数解析应使用当前会话的 `zoneId`

---

### 2. 修改 PrepareTask，移除 zoneId ✅

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/execution/config/session/PrepareTask.java`

**修改内容**：
- ❌ 移除了 `zoneId` 字段和参数
- ✅ 更新了构造函数，只接收 `statementName` 和 `sql`（AST）
- ✅ 添加了重复检查：如果 prepared statement 已存在，抛出异常

---

### 3. 修改 TableConfigTaskVisitor，移除 zoneId ✅

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/execution/config/TableConfigTaskVisitor.java`

**修改内容**：
- ✅ 修改 `visitPrepareStatement()` 方法，不再传递 `zoneId`

---

### 4. 创建 ParameterExtractor 工具类 ✅

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/ParameterExtractor.java`（新建）

**功能**：
- `extractParameters(Statement)`: 提取 AST 中的所有 `Parameter` 节点
- `bindParameters(Statement, List<Literal>)`: 创建 `Map<NodeRef<Parameter>, Expression>`，将 Parameter 节点映射到参数值

**参考**：Trino 的 `ParameterExtractor` 实现

---

### 5. 修改 TableModelPlanner 支持参数 ✅

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/planner/TableModelPlanner.java`

**修改内容**：
- ✅ 添加了 `parameters` 和 `parameterLookup` 字段
- ✅ 添加了重载构造函数，支持传入 `parameters` 和 `parameterLookup`
- ✅ 修改了 `analyze()` 方法，使用传入的 `parameters` 和 `parameterLookup`（而不是硬编码的空集合）

**向后兼容**：
- 保留了原有构造函数（无参数），内部调用新构造函数并传入空集合

---

### 6. 实现 Coordinator 中的 EXECUTE 处理 ✅

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/Coordinator.java`

**EXECUTE 处理流程**（参考 Trino 实现）：
```java
if (statement instanceof Execute) {
    // 1. 从会话获取 prepared statement（包含缓存的 AST）
    PreparedStatementInfo preparedInfo = clientSession.getPreparedStatement(statementName);
    Statement cachedStatement = preparedInfo.getSql(); // 缓存的 AST
    
    // 2. 绑定参数：创建 parameterLookup（类似 Trino 的 ParameterExtractor.bindParameters）
    Map<NodeRef<Parameter>, Expression> parameterLookup =
        ParameterExtractor.bindParameters(cachedStatement, executeStatement.getParameters());
    
    // 3. 使用缓存的 AST 和 parameterLookup 创建 QueryExecution（跳过 Parser）
    TableModelPlanner planner = new TableModelPlanner(
        cachedStatement, // 使用缓存的 AST
        ...,
        parameters,
        parameterLookup); // Analyzer 阶段使用 parameterLookup 解析参数
}
```

**EXECUTE IMMEDIATE 处理流程**：
```java
else if (statement instanceof ExecuteImmediate) {
    // 1. 解析 SQL（因为是字符串）
    Statement parsedStatement = sqlParser.createStatement(sql, ...);
    
    // 2. 如果有参数，绑定参数
    if (!parameters.isEmpty()) {
        parameterLookup = ParameterExtractor.bindParameters(parsedStatement, parameters);
    }
    
    // 3. 创建 QueryExecution
    TableModelPlanner planner = new TableModelPlanner(parsedStatement, ..., parameters, parameterLookup);
}
```

---

## 实现原理（参考 Trino）

### Trino 的实现方式

Trino **不在 AST 层面替换 Parameter 节点**，而是在 **Analyzer 阶段通过 parameterLookup 处理**：

1. **PREPARE 时**：
   - 解析 SQL → AST（包含 `Parameter` 节点）
   - 存储 AST 到 Session

2. **EXECUTE 时**：
   - 从 Session 获取缓存的 AST
   - 使用 `ParameterExtractor.bindParameters()` 创建 `parameterLookup`
   - 将 `parameterLookup` 传递给 `Analyzer`
   - `Analyzer` 在分析阶段通过 `parameterLookup` 解析 `Parameter` 节点

3. **关键代码**（Trino）：
```java
// SqlQueryExecution.java:278
Map<NodeRef<Parameter>, Expression> parameterLookup = 
    bindParameters(preparedQuery.getStatement(), preparedQuery.getParameters());

// Analyzer 使用 parameterLookup
Analyzer analyzer = analyzerFactory.createAnalyzer(
    session,
    preparedQuery.getParameters(),
    parameterLookup,  // ← 传递给 Analyzer
    ...);
```

### IoTDB 的实现方式（与 Trino 一致）

1. **PREPARE 时**：
   - 解析 SQL → AST（包含 `Parameter` 节点）
   - 存储 AST 到 `PreparedStatementInfo`

2. **EXECUTE 时**：
   - 从会话获取缓存的 AST（**跳过 Parser**）
   - 使用 `ParameterExtractor.bindParameters()` 创建 `parameterLookup`
   - 将 `parameterLookup` 传递给 `TableModelPlanner`
   - `TableModelPlanner.analyze()` 将 `parameterLookup` 传递给 `Analyzer`
   - `Analyzer` 在分析阶段通过 `parameterLookup` 解析 `Parameter` 节点（**保留 Analyzer**）

---

## 执行流程对比

### 优化前（设计文档中的计划）：
```
PREPARE stmt1 FROM 'SELECT * FROM table WHERE id = ?'
    ↓
1. SqlParser.createStatement() → AST
2. 存储 AST 到 PreparedStatementInfo

EXECUTE stmt1 USING 100
    ↓
1. 获取 PreparedStatementInfo（包含 AST）
2. AST → SQL 字符串（带 ?）
3. 字符串替换：? → 100
4. SqlParser.createStatement(replacedSql) ← ❌ 重新解析
5. Analyzer.analyze(statement)
```

### 优化后（参考 Trino）：
```
PREPARE stmt1 FROM 'SELECT * FROM table WHERE id = ?'
    ↓
1. SqlParser.createStatement() → AST（包含 Parameter 节点）
2. 存储 AST 到 PreparedStatementInfo

EXECUTE stmt1 USING 100
    ↓
1. 获取 PreparedStatementInfo（包含缓存的 AST）← ✅ 跳过 Parser
2. ParameterExtractor.bindParameters(AST, [100]) → parameterLookup
3. TableModelPlanner(..., parameters, parameterLookup)
4. Analyzer.analyze(statement) ← ✅ 保留 Analyzer
   - Analyzer 使用 parameterLookup 解析 Parameter 节点
```

---

## 关键优势

1. ✅ **跳过 Parser 阶段**：EXECUTE 时直接使用缓存的 AST，避免重复解析
2. ✅ **保留 Analyzer 阶段**：确保语义正确性（参数类型检查、权限验证等）
3. ✅ **与 Trino 一致**：使用相同的设计模式，便于理解和维护
4. ✅ **类型安全**：通过 `parameterLookup` 映射，避免字符串操作错误

---

## 修改的文件清单

1. ✅ **PreparedStatementInfo.java** - 移除 `zoneId`
2. ✅ **PrepareTask.java** - 移除 `zoneId`，添加重复检查
3. ✅ **TableConfigTaskVisitor.java** - 移除 `zoneId` 传递
4. ✅ **ParameterExtractor.java**（新建）- 参数提取和绑定工具类
5. ✅ **TableModelPlanner.java** - 支持 `parameters` 和 `parameterLookup`
6. ✅ **Coordinator.java** - 实现 EXECUTE 和 EXECUTE IMMEDIATE 处理

---

## 测试建议

1. **PREPARE 测试**：
   ```sql
   PREPARE stmt1 FROM 'SELECT * FROM table WHERE id = ?';
   -- 验证：stmt1 已存储，可以查询
   ```

2. **EXECUTE 测试**：
   ```sql
   EXECUTE stmt1 USING 100;
   -- 验证：使用缓存的 AST，跳过 Parser，保留 Analyzer
   ```

3. **EXECUTE IMMEDIATE 测试**：
   ```sql
   EXECUTE IMMEDIATE 'SELECT * FROM table WHERE id = 100';
   EXECUTE IMMEDIATE 'SELECT * FROM table WHERE id = ?' USING 100;
   ```

4. **错误处理测试**：
   - Prepared statement 不存在
   - 参数数量不匹配
   - 参数类型错误（在 Analyzer 阶段检查）

---

## 注意事项

1. **zoneId 处理**：
   - PREPARE 时不再存储 `zoneId`
   - EXECUTE 时使用当前会话的 `zoneId`（通过 `clientSession.getZoneId()`）

2. **参数绑定顺序**：
   - `ParameterExtractor.extractParameters()` 按出现顺序提取 Parameter 节点
   - `bindParameters()` 按顺序绑定参数值

3. **AST 不可变性**：
   - Parameter 节点不会被替换，而是在 Analyzer 阶段通过 `parameterLookup` 解析
   - 这确保了 AST 的不可变性，便于缓存和复用




