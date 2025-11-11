# ParameterExtractor.java 作用说明

## 概述

`ParameterExtractor` 是一个工具类，用于从 Prepared Statement 的 AST 中提取 `Parameter` 节点，并将参数值与 `Parameter` 节点进行绑定。它参考了 Trino 的 `ParameterExtractor` 实现。

---

## 主要功能

### 1. `getParameterCount(Statement statement)`

**作用**：获取语句中参数的数量。

**示例**：
```java
Statement stmt = sqlParser.createStatement("SELECT * FROM table WHERE id = ? AND name = ?");
int count = ParameterExtractor.getParameterCount(stmt); // 返回 2
```

---

### 2. `extractParameters(Statement statement)`

**作用**：从 AST 中提取所有 `Parameter` 节点，并按出现顺序返回。

**实现原理**：
- 使用 `ParameterExtractingVisitor`（继承自 `AstVisitor`）遍历 AST
- 在 `visitParameter()` 方法中收集所有 `Parameter` 节点
- 按照 `NodeLocation`（行号和列号）排序，确保顺序与 SQL 中的出现顺序一致

**示例**：
```java
Statement stmt = sqlParser.createStatement("SELECT * FROM table WHERE id = ? AND name = ?");
List<Parameter> parameters = ParameterExtractor.extractParameters(stmt);
// 返回：[Parameter(0), Parameter(1)]，按 SQL 中的出现顺序
```

**关键代码**：
```java:61:80:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/ParameterExtractor.java
public static List<Parameter> extractParameters(Statement statement) {
  ParameterExtractingVisitor visitor = new ParameterExtractingVisitor();
  visitor.process(statement, null);
  return visitor.getParameters().stream()
      .sorted(
          Comparator.comparing(
              parameter ->
                  parameter.getLocation()
                      .orElseThrow(
                          () ->
                              new SemanticException(
                                  "Parameter node must have a location")),
              Comparator.comparing(
                      org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NodeLocation
                          ::getLineNumber)
                  .thenComparing(
                      org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NodeLocation
                          ::getColumnNumber)))
      .collect(toImmutableList());
}
```

---

### 3. `bindParameters(Statement statement, List<Literal> values)`

**作用**：将参数值绑定到 `Parameter` 节点，创建 `Map<NodeRef<Parameter>, Expression>` 映射。

**参数**：
- `statement`：包含 `Parameter` 节点的 AST
- `values`：参数值列表（按顺序）

**返回值**：`Map<NodeRef<Parameter>, Expression>`，将每个 `Parameter` 节点映射到对应的 `Expression`（参数值）

**实现原理**：
1. 调用 `extractParameters()` 获取所有 `Parameter` 节点（按顺序）
2. 检查参数数量是否匹配
3. 按顺序将 `Parameter` 节点与参数值进行绑定
4. 使用 `NodeRef.of(parameter)` 作为 key，`Expression`（参数值）作为 value

**示例**：
```java
Statement stmt = sqlParser.createStatement("SELECT * FROM table WHERE id = ?");
List<Literal> values = Arrays.asList(new LongLiteral(100));

Map<NodeRef<Parameter>, Expression> parameterLookup = 
    ParameterExtractor.bindParameters(stmt, values);

// parameterLookup = {
//   NodeRef(Parameter(0)) -> LongLiteral(100)
// }
```

**关键代码**：
```java:91:108:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/ParameterExtractor.java
public static Map<NodeRef<Parameter>, Expression> bindParameters(
    Statement statement, List<Literal> values) {
  List<Parameter> parametersList = extractParameters(statement);

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

---

## 使用场景

### 场景 1：EXECUTE 语句处理

在 `Coordinator.createQueryExecutionForTableModel()` 中，处理 `EXECUTE` 语句时：

```java
// 1. 获取缓存的 AST（包含 Parameter 节点）
Statement cachedStatement = preparedInfo.getSql();

// 2. 绑定参数：创建 parameterLookup
Map<NodeRef<Parameter>, Expression> parameterLookup =
    ParameterExtractor.bindParameters(cachedStatement, executeStatement.getParameters());

// 3. 传递给 TableModelPlanner，Analyzer 阶段使用 parameterLookup 解析参数
TableModelPlanner planner = new TableModelPlanner(
    cachedStatement,
    ...,
    parameters,
    parameterLookup);
```

---

### 场景 2：EXECUTE IMMEDIATE 语句处理

在 `Coordinator.createQueryExecutionForTableModel()` 中，处理 `EXECUTE IMMEDIATE` 语句时：

```java
// 1. 解析 SQL（因为是字符串）
Statement parsedStatement = sqlParser.createStatement(sql, ...);

// 2. 如果有参数，绑定参数
if (!parameters.isEmpty()) {
  parameterLookup = ParameterExtractor.bindParameters(parsedStatement, parameters);
}
```

---

## 设计原理

### 为什么使用 `NodeRef<Parameter>` 作为 key？

1. **唯一性**：`NodeRef` 使用对象引用（`identityHashCode`）作为标识，确保每个 `Parameter` 节点都有唯一的引用
2. **不可变性**：`Parameter` 节点在 AST 中保持不变，`NodeRef` 可以安全地作为 Map 的 key
3. **与 Trino 一致**：Trino 也使用 `NodeRef<Parameter>` 作为 key

### 为什么按位置顺序绑定？

1. **SQL 标准**：参数按 `?` 在 SQL 中的出现顺序绑定
2. **用户友好**：`EXECUTE stmt1 USING 100, 'test'` 中，第一个值对应第一个 `?`，第二个值对应第二个 `?`
3. **实现简单**：按顺序遍历 `Parameter` 节点和参数值，一一对应

---

## 与 Trino 的对比

| 特性 | IoTDB | Trino |
|------|-------|-------|
| 提取参数 | `extractParameters()` | `extractParameters()` |
| 绑定参数 | `bindParameters()` | `bindParameters()` |
| 排序方式 | 按 `NodeLocation`（行号、列号） | 按 `NodeLocation`（行号、列号） |
| 返回类型 | `Map<NodeRef<Parameter>, Expression>` | `Map<NodeRef<Parameter>, Expression>` |

**结论**：IoTDB 的实现与 Trino 高度一致，便于理解和维护。

---

## 总结

`ParameterExtractor` 的核心作用是：

1. ✅ **提取参数**：从 AST 中提取所有 `Parameter` 节点
2. ✅ **绑定参数**：将参数值与 `Parameter` 节点进行绑定，创建 `parameterLookup` 映射
3. ✅ **支持 Analyzer**：为 `Analyzer` 阶段提供参数查找映射，实现参数替换

**关键优势**：
- 保持 AST 不可变性（不修改 AST 节点）
- 支持跳过 Parser 阶段（EXECUTE 时使用缓存的 AST）
- 在 Analyzer 阶段进行类型检查，确保参数类型正确




