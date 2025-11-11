# Prepared Statement 优化分析

## 1. zoneId 的必要性分析

### 1.1 当前实现

```java
public class PreparedStatementInfo {
    private final String statementName;
    private final Statement sql;  // AST
    private final ZoneId zoneId;  // ❓ 是否有必要？
    private final long createTime;
}
```

### 1.2 zoneId 的作用

`zoneId` 在 SQL 解析时用于：
1. **时间字面量解析**：如 `TIMESTAMP '2024-01-01 10:00:00'` 需要 zoneId 来解析
2. **日期函数**：如 `NOW()`, `CURRENT_TIMESTAMP()` 需要 zoneId
3. **时间戳转换**：字符串到时间戳的转换

### 1.3 问题分析

**问题 1：执行时的 zoneId 可能不同**
- PREPARE 时的 zoneId：`ZoneId.of("UTC")`
- EXECUTE 时的 zoneId：`ZoneId.of("Asia/Shanghai")`（会话可能改变了时区）
- **应该使用哪个 zoneId？**

**问题 2：AST 中已经包含时区信息**
- 如果 SQL 中包含时间字面量，解析时已经使用了 zoneId
- AST 中的时间字面量已经是特定时区的值
- 后续执行时，应该使用**当前会话的 zoneId**，而不是 PREPARE 时的 zoneId

**问题 3：参数中的时间值**
- 如果参数是时间值（如 `EXECUTE stmt1 USING TIMESTAMP '2024-01-01'`）
- 参数解析时应该使用**当前会话的 zoneId**，而不是 PREPARE 时的 zoneId

### 1.4 结论

**zoneId 不应该存储在 PreparedStatementInfo 中**，原因：

1. **执行时应该使用当前会话的 zoneId**：
   ```java
   // EXECUTE 时
   IClientSession session = SessionManager.getInstance().getCurrSession();
   ZoneId currentZoneId = session.getZoneId();  // 使用当前会话的 zoneId
   ```

2. **AST 中的时间字面量已经固定**：
   - PREPARE 时解析的时间字面量已经使用了当时的 zoneId
   - 如果时区改变，应该重新 PREPARE

3. **参数解析使用当前时区**：
   - `EXECUTE stmt1 USING TIMESTAMP '2024-01-01'` 中的参数应该使用当前会话的 zoneId

### 1.5 建议修改

**移除 PreparedStatementInfo 中的 zoneId**：

```java
public class PreparedStatementInfo {
    private final String statementName;
    private final Statement sql;  // AST（已解析，包含时间字面量的时区信息）
    private final long createTime;  // 创建时间（用于调试/监控）
    
    // 移除 zoneId
}
```

**修改 PrepareTask**：

```java
public class PrepareTask implements IConfigTask {
    private final String statementName;
    private final Statement sql;
    // 移除 zoneId 参数
    
    public PrepareTask(String statementName, Statement sql) {
        this.statementName = statementName;
        this.sql = sql;
    }
    
    @Override
    public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
        // ...
        PreparedStatementInfo info = new PreparedStatementInfo(statementName, sql);
        // 不再传递 zoneId
    }
}
```

---

## 2. AST 缓存和复用（跳过 Parser 阶段）

### 2.1 当前设计问题

**当前流程**（设计文档中的计划）：
```
EXECUTE stmt1 USING 100
    ↓
1. 从会话获取 PreparedStatementInfo（包含 AST）
2. 将 AST 转换为 SQL 字符串（带占位符）
3. 字符串替换：将 ? 替换为 100
4. 重新解析：SqlParser.createStatement(replacedSql)  ← ❌ 重复解析
5. Analyzer.analyze(statement)
```

**问题**：
- ❌ **重复解析**：PREPARE 时已经解析为 AST，EXECUTE 时又重新解析
- ❌ **性能损失**：Parser 阶段的开销被浪费
- ❌ **字符串操作**：AST → SQL → AST 的转换效率低

### 2.2 优化方案：AST 层面的参数替换

**优化后的流程**：
```
EXECUTE stmt1 USING 100
    ↓
1. 从会话获取 PreparedStatementInfo（包含 AST）
2. AST 参数替换：在 AST 中直接替换占位符节点 ← ✅ 跳过 Parser
3. Analyzer.analyze(replacedStatement)  ← ✅ 保留 Analyzer
```

### 2.3 实现方案

#### 2.3.1 创建 AST 参数替换器

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/ast/ParameterReplacer.java`

```java
package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.visitor.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.visitor.DefaultAstVisitor;

import java.util.List;

/**
 * AST 参数替换器：在 AST 中直接替换占位符节点，避免重新解析。
 */
public class ParameterReplacer {
    
    /**
     * 替换 AST 中的占位符节点为实际参数值
     * 
     * @param statement 原始 AST（包含占位符）
     * @param parameters 参数值列表
     * @return 替换后的 AST
     */
    public static Statement replaceParameters(Statement statement, List<Literal> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return statement;
        }
        
        ParameterReplacerVisitor visitor = new ParameterReplacerVisitor(parameters);
        return (Statement) visitor.process(statement, null);
    }
    
    private static class ParameterReplacerVisitor extends DefaultAstVisitor<Node, Void> {
        private final List<Literal> parameters;
        private int parameterIndex = 0;
        
        public ParameterReplacerVisitor(List<Literal> parameters) {
            this.parameters = parameters;
        }
        
        @Override
        protected Node visitParameter(Parameter node, Void context) {
            int paramId = node.getId();  // Parameter 使用 id 字段（从 0 开始）
            
            if (paramId < 0 || paramId >= parameters.size()) {
                throw new SemanticException(
                    String.format("Parameter index out of range: parameter %d, but only %d parameters provided", 
                        paramId, parameters.size()));
            }
            
            Literal parameter = parameters.get(paramId);
            
            // 验证参数类型（可选）
            // validateParameterType(node, parameter);
            
            return parameter;
        }
        
        @Override
        protected Node visitNode(Node node, Void context) {
            // 递归处理所有子节点
            return node.accept(this, context);
        }
    }
}
```

**注意**：`Parameter` AST 节点已经存在（`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/ast/Parameter.java`），使用 `id` 字段（从 0 开始）标识参数位置。

#### 2.3.2 修改 Coordinator 中的 EXECUTE 处理

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/Coordinator.java`

```java
// Handle EXECUTE and EXECUTE IMMEDIATE statements
if (statement instanceof Execute) {
    Execute executeStatement = (Execute) statement;
    String statementName = executeStatement.getStatementName().getValue();
    
    // 1. 从会话获取 prepared statement
    PreparedStatementInfo preparedInfo = clientSession.getPreparedStatement(statementName);
    if (preparedInfo == null) {
        throw new SemanticException(
            String.format("Prepared statement '%s' does not exist", statementName));
    }
    
    // 2. 获取缓存的 AST
    Statement cachedStatement = preparedInfo.getSql();
    
    // 3. AST 层面的参数替换（跳过 Parser）
    Statement replacedStatement = ParameterReplacer.replaceParameters(
        cachedStatement, 
        executeStatement.getParameters());
    
    // 4. 递归调用，使用替换后的 AST 创建 QueryExecution
    return createQueryExecutionForTableModel(
        replacedStatement,
        sqlParser,
        clientSession,
        queryContext,
        metadata,
        timeOut,
        startTime);
    
} else if (statement instanceof ExecuteImmediate) {
    ExecuteImmediate executeImmediateStatement = (ExecuteImmediate) statement;
    
    // EXECUTE IMMEDIATE 需要解析 SQL，但如果有参数，也需要替换
    String sql = executeImmediateStatement.getSqlString();
    List<Literal> parameters = executeImmediateStatement.getParameters();
    
    if (!parameters.isEmpty()) {
        // 如果有参数，先解析 SQL，然后替换参数
        Statement parsedStatement = sqlParser.createStatement(
            sql, 
            clientSession.getZoneId(), 
            clientSession);
        
        Statement replacedStatement = ParameterReplacer.replaceParameters(
            parsedStatement, 
            parameters);
        
        return createQueryExecutionForTableModel(
            replacedStatement,
            sqlParser,
            clientSession,
            queryContext,
            metadata,
            timeOut,
            startTime);
    } else {
        // 没有参数，直接解析
        Statement parsedStatement = sqlParser.createStatement(
            sql, 
            clientSession.getZoneId(), 
            clientSession);
        
        return createQueryExecutionForTableModel(
            parsedStatement,
            sqlParser,
            clientSession,
            queryContext,
            metadata,
            timeOut,
            startTime);
    }
}
```

#### 2.3.3 Parameter AST 节点已存在

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/ast/Parameter.java`

`Parameter` 节点已经存在，使用 `id` 字段（从 0 开始）标识参数位置：

```java
public class Parameter extends Expression {
    private final int id;  // 参数 ID（从 0 开始）
    
    public int getId() {
        return id;
    }
    
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitParameter(this, context);
    }
}
```

**注意**：`AstVisitor` 中已经有 `visitParameter` 方法。

### 2.4 执行流程对比

#### 当前设计（字符串替换 + 重新解析）：
```
PREPARE stmt1 FROM 'SELECT * FROM table WHERE id = ?'
    ↓
1. SqlParser.createStatement() → AST (包含 Parameter 节点)
2. 存储 AST 到 PreparedStatementInfo

EXECUTE stmt1 USING 100
    ↓
1. 获取 PreparedStatementInfo（包含 AST）
2. AST → SQL 字符串（带 ?）
3. 字符串替换：? → 100
4. SqlParser.createStatement(replacedSql) ← ❌ 重新解析
5. Analyzer.analyze(statement)
```

#### 优化后（AST 层面替换）：
```
PREPARE stmt1 FROM 'SELECT * FROM table WHERE id = ?'
    ↓
1. SqlParser.createStatement() → AST (包含 Parameter 节点)
2. 存储 AST 到 PreparedStatementInfo

EXECUTE stmt1 USING 100
    ↓
1. 获取 PreparedStatementInfo（包含 AST）
2. ParameterReplacer.replaceParameters(AST, [100]) ← ✅ AST 层面替换
   - 遍历 AST，找到 Parameter 节点
   - 替换为 Literal(100)
3. Analyzer.analyze(replacedStatement) ← ✅ 跳过 Parser，保留 Analyzer
```

### 2.5 性能优势

| 阶段 | 当前设计 | 优化后 | 节省 |
|------|---------|--------|------|
| **Parser** | 每次执行都解析 | 只在 PREPARE 时解析 | ✅ 跳过 |
| **AST 替换** | 无 | AST 遍历替换 | 新增（但很快） |
| **Analyzer** | 每次执行都分析 | 每次执行都分析 | 保留 |

**性能提升**：
- ✅ **跳过 Parser 阶段**：避免词法分析、语法分析的开销
- ✅ **保留 Analyzer 阶段**：确保语义正确性（参数类型检查、权限验证等）
- ✅ **AST 操作高效**：直接修改 AST 节点，无需字符串操作

### 2.6 需要修改的文件清单

1. **PreparedStatementInfo.java**
   - 移除 `zoneId` 字段

2. **PrepareTask.java**
   - 移除 `zoneId` 参数
   - 修改构造函数和 `execute()` 方法

3. **TableConfigTaskVisitor.java**
   - 修改 `visitPrepareStatement()` 方法，不再传递 zoneId

4. **ParameterReplacer.java**（新建）
   - 实现 AST 层面的参数替换

5. **Parameter.java**（已存在）
   - ✅ `Parameter` 节点已存在，使用 `id` 字段标识参数位置

6. **AstVisitor.java**（已存在）
   - ✅ `visitParameter()` 方法已存在

7. **AstBuilder.java**
   - 在解析 `?` 时创建 `Parameter` 节点

8. **Coordinator.java**
   - 修改 `createQueryExecutionForTableModel()` 中的 EXECUTE 处理逻辑
   - 使用 `ParameterReplacer` 替换参数，而不是字符串替换 + 重新解析

---

## 3. 总结

### 3.1 zoneId 处理

- ❌ **移除** `PreparedStatementInfo` 中的 `zoneId` 字段
- ✅ **使用** 当前会话的 `zoneId` 进行参数解析和执行

### 3.2 AST 缓存和复用

- ✅ **PREPARE 时**：解析 SQL → AST，存储到 `PreparedStatementInfo`
- ✅ **EXECUTE 时**：
  - 从会话获取缓存的 AST
  - 使用 `ParameterReplacer` 在 AST 层面替换参数
  - **跳过 Parser 阶段**
  - **保留 Analyzer 阶段**（确保语义正确性）

### 3.3 关键优势

1. **性能提升**：跳过 Parser 阶段，减少重复解析开销
2. **类型安全**：AST 层面的参数替换，避免字符串操作错误
3. **语义正确**：保留 Analyzer 阶段，确保参数类型检查和权限验证

