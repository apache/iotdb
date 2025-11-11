# 参数替换发生的位置

## 问题 2：参数替换目前是在哪里发生的？

### 答案：参数替换发生在 **Analyzer 阶段**，具体位置是 `ExpressionAnalyzer.visitParameter()` 方法

---

## 详细说明

### 1. 参数替换的流程

```
EXECUTE stmt1 USING 100
    ↓
1. Coordinator.createQueryExecutionForTableModel()
   - 获取缓存的 AST（包含 Parameter 节点）
   - ParameterExtractor.bindParameters() → parameterLookup
   - 创建 TableModelPlanner(..., parameters, parameterLookup)
    ↓
2. TableModelPlanner.analyze()
   - 创建 Analyzer(..., parameters, parameterLookup)
   - analyzer.analyze(statement)
    ↓
3. Analyzer.analyze()
   - 创建 StatementAnalyzer
   - statementAnalyzer.analyze(statement)
    ↓
4. StatementAnalyzer.analyze()
   - 创建 ExpressionAnalyzer(..., parameters)
   - expressionAnalyzer.analyze(expression)
    ↓
5. ExpressionAnalyzer.visitParameter() ← ✅ 参数替换发生在这里
   - parameters.get(NodeRef.of(node)) → 获取对应的 Expression
   - process(providedValue, context) → 对参数值进行类型分析
```

---

### 2. 关键代码位置

#### 2.1 参数绑定（Coordinator）

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/Coordinator.java`

```java:514:517:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/Coordinator.java
// 3. Bind parameters: create parameterLookup map (similar to Trino's ParameterExtractor.bindParameters)
// This allows Analyzer to resolve Parameter nodes without re-parsing
Map<NodeRef<Parameter>, Expression> parameterLookup =
    ParameterExtractor.bindParameters(cachedStatement, executeStatement.getParameters());
```

**作用**：创建 `parameterLookup` 映射，将 `Parameter` 节点映射到对应的 `Expression` 值。

---

#### 2.2 参数替换（ExpressionAnalyzer）

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/analyzer/ExpressionAnalyzer.java`

```java:1540:1558:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/analyzer/ExpressionAnalyzer.java
@Override
protected Type visitParameter(Parameter node, StackableAstVisitorContext<Context> context) {

  if (parameters.isEmpty()) {
    throw new SemanticException("Query takes no parameters");
  }
  if (node.getId() >= parameters.size()) {
    throw new SemanticException(
        String.format(
            "Invalid parameter index %s, max value is %s",
            node.getId(), parameters.size() - 1));
  }

  Expression providedValue = parameters.get(NodeRef.of(node));  // ← 从 parameterLookup 获取参数值
  if (providedValue == null) {
    throw new SemanticException("No value provided for parameter");
  }
  Type resultType = process(providedValue, context);  // ← 对参数值进行类型分析
  return setExpressionType(node, resultType);
}
```

**关键点**：
1. `parameters` 字段实际上是 `Map<NodeRef<Parameter>, Expression>`（即 `parameterLookup`）
2. `parameters.get(NodeRef.of(node))` 从 `parameterLookup` 中获取对应的 `Expression` 值
3. `process(providedValue, context)` 对参数值进行类型分析（递归分析参数值的类型）

---

### 3. 参数替换的本质

**重要**：参数替换**不是在 AST 层面替换 Parameter 节点**，而是：

1. **AST 保持不变**：`Parameter` 节点仍然存在于 AST 中
2. **Analyzer 阶段解析**：在 `ExpressionAnalyzer.visitParameter()` 中，通过 `parameterLookup` 获取对应的 `Expression` 值
3. **类型分析**：对参数值进行类型分析，确定 `Parameter` 节点的类型

---

### 4. 为什么不在 AST 层面替换？

**优势**：
1. ✅ **AST 不可变性**：保持 AST 的不可变性，便于缓存和复用
2. ✅ **类型安全**：在 Analyzer 阶段进行类型检查，确保参数类型正确
3. ✅ **与 Trino 一致**：使用相同的设计模式，便于理解和维护
4. ✅ **跳过 Parser**：EXECUTE 时直接使用缓存的 AST，避免重新解析

---

### 5. 数据流图

```
PREPARE stmt1 FROM 'SELECT * FROM table WHERE id = ?'
    ↓
AST: Select(... Where(Comparison(id, Parameter(0))))
    ↓
存储到 PreparedStatementInfo（AST 包含 Parameter 节点）

EXECUTE stmt1 USING 100
    ↓
1. 获取缓存的 AST（包含 Parameter(0)）
2. ParameterExtractor.bindParameters()
   → parameterLookup = {NodeRef(Parameter(0)) → LongLiteral(100)}
3. Analyzer.analyze(statement)
   ↓
4. ExpressionAnalyzer.visitParameter(Parameter(0))
   → providedValue = parameterLookup.get(NodeRef(Parameter(0))) = LongLiteral(100)
   → process(LongLiteral(100)) → Type = INT64
   → setExpressionType(Parameter(0), INT64)
    ↓
5. 后续分析使用 Parameter(0) 的类型（INT64）进行语义分析
```

---

## 总结

- **参数替换位置**：`ExpressionAnalyzer.visitParameter()` 方法（第 1540-1558 行）
- **替换方式**：通过 `parameterLookup` 在 Analyzer 阶段获取参数值，而不是替换 AST 节点
- **优势**：保持 AST 不可变性，跳过 Parser，保留类型检查




