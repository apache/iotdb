# parameterLookup 作用域和必要性分析

## 问题

用户提出：`parameterLookup` 是否只针对一个 Statement 的不同位置？会不会出现两个查询模板往一个 Map 里面插入？`parameterLookup` 是全局的参数还是当前模板的参数？如果是当前模板的话，好像就没有必要了。

## 分析

### 1. parameterLookup 的作用域

**结论**：`parameterLookup` 是**每个 Statement 实例**的，不是全局的。

#### 1.1 创建时机

`parameterLookup` 在 `Coordinator.createQueryExecutionForTableModel()` 中为**每个 EXECUTE 语句**创建：

```java:514:518:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/Coordinator.java
// 3. Bind parameters: create parameterLookup map (similar to Trino's ParameterExtractor.bindParameters)
// This allows Analyzer to resolve Parameter nodes without re-parsing
// Note: bindParameters() internally validates parameter count
Map<NodeRef<Parameter>, Expression> parameterLookup =
    ParameterExtractor.bindParameters(cachedStatement, executeStatement.getParameters());
```

#### 1.2 生命周期

```
EXECUTE stmt1 USING 100
    ↓
1. Coordinator.createQueryExecutionForTableModel()
   → 创建新的 parameterLookup（只包含当前 Statement 的 Parameter 节点）
   → 创建新的 TableModelPlanner(..., parameterLookup)
   → 创建新的 QueryExecution(tableModelPlanner)
    ↓
2. QueryExecution 执行
   → TableModelPlanner.analyze() 创建新的 Analyzer(..., parameterLookup)
   → Analyzer 使用 parameterLookup 分析当前 Statement
    ↓
3. QueryExecution 完成，所有对象被回收
```

**关键点**：
- 每个 `EXECUTE` 语句都会创建**新的** `QueryExecution`
- 每个 `QueryExecution` 都有**自己的** `TableModelPlanner`
- 每个 `TableModelPlanner` 都有**自己的** `Analyzer`
- 每个 `Analyzer` 都有**自己的** `parameterLookup`

#### 1.3 不会跨 Statement 共享

**场景 1：同一个 PreparedStatement 多次执行**

```java
// 第一次执行
EXECUTE stmt1 USING 100
→ 创建 QueryExecution1
  → parameterLookup1 = {NodeRef(Parameter(0)) → LongLiteral(100)}
  → Analyzer1 使用 parameterLookup1

// 第二次执行（不同的参数值）
EXECUTE stmt1 USING 200
→ 创建 QueryExecution2（新的实例）
  → parameterLookup2 = {NodeRef(Parameter(0)) → LongLiteral(200)}
  → Analyzer2 使用 parameterLookup2（独立的实例）
```

**场景 2：不同的 PreparedStatement**

```java
// 执行 stmt1
EXECUTE stmt1 USING 100
→ QueryExecution1
  → parameterLookup1 = {NodeRef(Parameter(0)) → LongLiteral(100)}
  → 只包含 stmt1 的 Parameter 节点

// 执行 stmt2（同时或之后）
EXECUTE stmt2 USING 'test'
→ QueryExecution2（新的实例）
  → parameterLookup2 = {NodeRef(Parameter(0)) → StringLiteral('test')}
  → 只包含 stmt2 的 Parameter 节点
  → 与 parameterLookup1 完全独立
```

### 2. parameterLookup 的必要性

**结论**：`parameterLookup` 是**必要的**，即使只针对当前 Statement。

#### 2.1 为什么需要 NodeRef？

虽然 `parameterLookup` 只包含当前 Statement 的 Parameter 节点，但使用 `NodeRef<Parameter>` 作为 key 仍然必要：

**原因 1：AST 节点身份唯一性**

即使同一个 Statement 中，每个 `Parameter` 节点都是**不同的对象实例**：

```java
// Statement: SELECT * FROM t WHERE a = ? AND b = ?
// AST 结构：
//   Select(...)
//     Where(...)
//       And(...)
//         Comparison(a, Parameter(0))  ← Parameter 实例 1
//         Comparison(b, Parameter(1))  ← Parameter 实例 2
```

- `Parameter(0)` 和 `Parameter(1)` 是不同的对象实例
- 即使它们的 `id` 相同（都是 0），它们也是不同的对象
- `NodeRef` 使用对象引用（`identityHashCode`）确保唯一性

**原因 2：避免使用 id 作为 key 的问题**

如果使用 `id` 作为 key（`Map<Integer, Expression>`），会有问题：

```java
// 假设使用 id 作为 key
Map<Integer, Expression> parameterLookup = new HashMap<>();
parameterLookup.put(0, LongLiteral(100));
parameterLookup.put(1, StringLiteral("test"));

// 在 ExpressionAnalyzer.visitParameter() 中：
Expression value = parameterLookup.get(node.getId()); // 使用 id 查找
```

**问题**：
- 如果 Statement 中有多个相同 `id` 的 `Parameter` 节点（虽然当前实现不会出现，但设计上需要考虑）
- 如果 AST 被复制或转换，可能创建新的 `Parameter` 对象，但 `id` 相同
- 使用 `NodeRef` 可以确保每个节点实例都有唯一的引用

**原因 3：与 Trino 保持一致**

Trino 也使用 `NodeRef<Parameter>` 作为 key，这是经过验证的设计模式。

#### 2.2 当前实现的分析

查看 `ParameterExtractor.bindParameters()`：

```java:91:109:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/ParameterExtractor.java
public static Map<NodeRef<Parameter>, Expression> bindParameters(
    Statement statement, List<Literal> values) {
  List<Parameter> parametersList = extractParameters(statement);

  // Validate parameter count
  if (parametersList.size() != values.size()) {
    throw new SemanticException(
        String.format(
            "Invalid number of parameters: expected %d, got %d",
            parametersList.size(), values.size()));
  }

  ImmutableMap.Builder<NodeRef<Parameter>, Expression> builder = ImmutableMap.builder();
  Iterator<Literal> iterator = values.iterator();
  for (Parameter parameter : parametersList) {
    builder.put(NodeRef.of(parameter), iterator.next());
  }
  return builder.buildOrThrow();
}
```

**关键点**：
- `extractParameters(statement)` 从**单个 Statement** 中提取所有 `Parameter` 节点
- 每个 `Parameter` 节点都被包装为 `NodeRef.of(parameter)`
- 创建的 Map 只包含**当前 Statement** 的 Parameter 节点

#### 2.3 在 Analyzer 中的使用

查看 `ExpressionAnalyzer.visitParameter()`：

```java
@Override
protected Type visitParameter(Parameter node, StackableAstVisitorContext<Context> context) {
  // ...
  Expression providedValue = parameters.get(NodeRef.of(node));  // ← 使用 NodeRef 查找
  // ...
}
```

**关键点**：
- `parameters` 字段实际上是 `Map<NodeRef<Parameter>, Expression>`（即 `parameterLookup`）
- 使用 `NodeRef.of(node)` 作为 key 查找对应的值
- 确保每个 `Parameter` 节点实例都能找到对应的值

### 3. 是否可以简化？

**问题**：既然 `parameterLookup` 只包含当前 Statement 的 Parameter 节点，是否可以直接使用 `id` 作为 key？

**答案**：理论上可以，但使用 `NodeRef` 更安全、更一致。

#### 3.1 使用 id 的方案（理论上可行）

```java
// 方案 1：使用 id 作为 key
Map<Integer, Expression> parameterLookup = new HashMap<>();
for (int i = 0; i < parametersList.size(); i++) {
  parameterLookup.put(parametersList.get(i).getId(), values.get(i));
}

// 在 ExpressionAnalyzer.visitParameter() 中：
Expression value = parameterLookup.get(node.getId());
```

**优点**：
- 更简单
- 内存占用更小（Integer vs NodeRef）

**缺点**：
- 如果 Statement 中有多个相同 `id` 的 `Parameter` 节点（虽然当前不会出现），会有问题
- 与 Trino 不一致
- 如果 AST 被复制，可能出现 `id` 冲突

#### 3.2 使用 NodeRef 的方案（当前实现）

**优点**：
- 确保每个节点实例的唯一性
- 与 Trino 保持一致
- 更安全，避免潜在的冲突

**缺点**：
- 稍微复杂一些
- 内存占用稍大（但可以忽略）

### 4. 结论

1. **作用域**：`parameterLookup` 是**每个 Statement 实例**的，不会跨 Statement 共享
2. **必要性**：即使只针对当前 Statement，使用 `NodeRef<Parameter>` 作为 key 仍然是**必要的**和**推荐的**
3. **原因**：
   - 确保节点实例的唯一性
   - 与 Trino 保持一致
   - 更安全，避免潜在的冲突

### 5. 数据流验证

```
PREPARE stmt1 FROM 'SELECT * FROM t WHERE a = ? AND b = ?'
→ AST: Select(... Where(... And(... Comparison(a, Parameter(0)), Comparison(b, Parameter(1)))))
→ 存储到 PreparedStatementInfo

EXECUTE stmt1 USING 100, 'test'
→ 1. 获取缓存的 AST（包含 Parameter(0) 和 Parameter(1)）
→ 2. ParameterExtractor.bindParameters()
   → extractParameters() 提取 Parameter(0) 和 Parameter(1)
   → 创建 parameterLookup = {
       NodeRef(Parameter(0)) → LongLiteral(100),
       NodeRef(Parameter(1)) → StringLiteral('test')
     }
→ 3. Analyzer.analyze(statement)
   → ExpressionAnalyzer.visitParameter(Parameter(0))
     → parameterLookup.get(NodeRef.of(Parameter(0))) → LongLiteral(100)
   → ExpressionAnalyzer.visitParameter(Parameter(1))
     → parameterLookup.get(NodeRef.of(Parameter(1))) → StringLiteral('test')
```

**验证**：
- `parameterLookup` 只包含当前 Statement 的 Parameter 节点
- 每个 `Parameter` 节点都有唯一的 `NodeRef`
- 不会出现跨 Statement 的冲突

### 6. 建议

**保持当前实现**：
- ✅ `parameterLookup` 是每个 Statement 实例的（正确）
- ✅ 使用 `NodeRef<Parameter>` 作为 key（推荐）
- ✅ 与 Trino 保持一致（便于理解和维护）

**如果确实想简化**（不推荐）：
- 可以使用 `id` 作为 key，但需要确保 Statement 中不会有相同 `id` 的 `Parameter` 节点
- 需要修改 `ExpressionAnalyzer.visitParameter()` 使用 `node.getId()` 而不是 `NodeRef.of(node)`
- 失去与 Trino 的一致性




