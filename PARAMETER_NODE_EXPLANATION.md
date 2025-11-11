# Parameter 节点创建和使用说明

## 1. Parameter 在哪里被创建？

`Parameter` 节点在 **SQL 解析阶段**（Parser）被创建，具体位置在 `AstBuilder.java` 中。

### 创建位置

**文件**：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/parser/AstBuilder.java`

#### 1.1 主要创建方法：`visitParameter()`

```java:3656:3660:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/parser/AstBuilder.java
@Override
public Node visitParameter(RelationalSqlParser.ParameterContext ctx) {
  Parameter parameter = new Parameter(getLocation(ctx), parameterPosition);
  parameterPosition++;
  return parameter;
}
```

**触发时机**：当 SQL 中包含 `?` 占位符时，ANTLR 解析器会调用此方法。

#### 1.2 其他创建位置

`Parameter` 也会在以下场景中被创建：

**`visitLimit()`** - 当 LIMIT 子句使用 `?` 作为参数时：
```java:2230:2232:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/parser/AstBuilder.java
rowCount = new Parameter(getLocation(ctx.rowCount().QUESTION_MARK()), parameterPosition);
parameterPosition++;
```

**`visitOffset()`** - 当 OFFSET 子句使用 `?` 作为参数时：
```java:2243:2245:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/parser/AstBuilder.java
rowCount = new Parameter(getLocation(ctx.QUESTION_MARK()), parameterPosition);
parameterPosition++;
```

### parameterPosition 变量

`parameterPosition` 是 `AstBuilder` 类的成员变量：

```java:343:343:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/parser/AstBuilder.java
private int parameterPosition;
```

**作用**：
- 跟踪当前参数的位置索引（从 0 开始）
- 每次创建 `Parameter` 节点时，使用当前 `parameterPosition` 值作为 `id`
- 创建后自动递增，确保每个参数都有唯一的 `id`

**示例**：
```sql
SELECT * FROM t WHERE a = ? AND b = ?
```
- 第一个 `?` → `new Parameter(location, 0)` → `parameterPosition` 变为 1
- 第二个 `?` → `new Parameter(location, 1)` → `parameterPosition` 变为 2

---

## 2. Parameter 的 id 字段是什么意思？

`id` 字段表示**参数在 SQL 中出现的顺序位置**（从 0 开始）。

### 定义

```java:30:44:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/ast/Parameter.java
public class Parameter extends Expression {
  private final int id;

  public Parameter(int id) {
    super(null);
    this.id = id;
  }

  public Parameter(NodeLocation location, int id) {
    super(requireNonNull(location, "location is null"));
    this.id = id;
  }

  public int getId() {
    return id;
  }
```

### id 的含义

1. **位置索引**：`id` 表示参数在 SQL 语句中出现的顺序
   - 第一个 `?` 的 `id = 0`
   - 第二个 `?` 的 `id = 1`
   - 以此类推

2. **与参数值的对应关系**：
   ```sql
   PREPARE stmt1 FROM 'SELECT * FROM t WHERE a = ? AND b = ?';
   EXECUTE stmt1 USING 100, 'test';
   ```
   - `id = 0` 的 `Parameter` 对应值 `100`
   - `id = 1` 的 `Parameter` 对应值 `'test'`

3. **用于 equals() 和 hashCode()**：
   ```java:56:72:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/sql/ast/Parameter.java
   @Override
   public boolean equals(Object o) {
     if (this == o) {
       return true;
     }
     if (o == null || getClass() != o.getClass()) {
       return false;
     }

     Parameter that = (Parameter) o;
     return Objects.equals(id, that.id);
   }

   @Override
   public int hashCode() {
     return id;
   }
   ```
   **注意**：`Parameter` 的 `equals()` 和 `hashCode()` 是基于 `id` 的，这意味着两个相同 `id` 的 `Parameter` 对象会被认为是相等的。

---

## 3. 为什么不能直接使用 Parameter 作为 key，而要用 NodeRef<Parameter>？

### 问题背景

在 `ParameterExtractor.bindParameters()` 中，我们使用 `Map<NodeRef<Parameter>, Expression>` 作为 `parameterLookup`，而不是 `Map<Parameter, Expression>`。

### 原因分析

#### 3.1 Parameter 的 equals() 基于 id

`Parameter` 的 `equals()` 方法只比较 `id`：

```java
@Override
public boolean equals(Object o) {
  Parameter that = (Parameter) o;
  return Objects.equals(id, that.id);  // 只比较 id
}
```

这意味着：
- `new Parameter(location1, 0)` 和 `new Parameter(location2, 0)` 被认为是**相等的**
- 即使它们是**不同的对象实例**，在 AST 中位于**不同的位置**

#### 3.2 AST 中可能出现相同 id 的 Parameter

在某些复杂场景中，同一个 `id` 的 `Parameter` 可能出现在 AST 的不同位置：

**示例场景**（虽然 IoTDB 当前可能不支持，但设计上需要考虑）：
```sql
-- 假设支持子查询中的参数
SELECT * FROM t1 WHERE a = ? 
  AND EXISTS (SELECT 1 FROM t2 WHERE b = ?)
```
- 第一个 `?` 的 `id = 0`
- 第二个 `?` 的 `id = 1`
- 但如果子查询中的参数也使用全局计数，可能出现相同 `id` 的情况

**更实际的场景**：
- AST 的复制或转换过程中，可能创建相同 `id` 的新 `Parameter` 对象
- 如果直接使用 `Parameter` 作为 Map 的 key，这些不同的对象实例会被认为是同一个 key，导致参数绑定错误

#### 3.3 NodeRef 使用对象引用作为标识

`NodeRef` 使用**对象引用**（`identityHashCode`）作为标识：

```java:42:57:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/analyzer/NodeRef.java
@Override
public boolean equals(Object o) {
  if (this == o) {
    return true;
  }
  if (o == null || getClass() != o.getClass()) {
    return false;
  }
  NodeRef<?> other = (NodeRef<?>) o;
  return node == other.node;  // 使用 == 比较对象引用
}

@Override
public int hashCode() {
  return identityHashCode(node);  // 使用对象引用的哈希码
}
```

**优势**：
1. ✅ **唯一性**：每个 `Parameter` 节点实例都有唯一的引用，即使它们的 `id` 相同
2. ✅ **精确匹配**：确保 Map 中的 key 精确对应 AST 中的特定节点实例
3. ✅ **安全性**：避免因 `equals()` 导致的错误匹配

### 对比示例

**使用 Parameter 作为 key（有问题）**：
```java
Map<Parameter, Expression> map = new HashMap<>();
Parameter p1 = new Parameter(location1, 0);
Parameter p2 = new Parameter(location2, 0);  // 不同的对象，但 id 相同

map.put(p1, value1);
map.get(p2);  // ❌ 返回 value1（错误！p2 和 p1 被认为是相等的）
```

**使用 NodeRef<Parameter> 作为 key（正确）**：
```java
Map<NodeRef<Parameter>, Expression> map = new HashMap<>();
Parameter p1 = new Parameter(location1, 0);
Parameter p2 = new Parameter(location2, 0);  // 不同的对象，id 相同

map.put(NodeRef.of(p1), value1);
map.get(NodeRef.of(p2));  // ✅ 返回 null（正确！p1 和 p2 是不同的对象引用）
```

### 实际使用

在 `ParameterExtractor.bindParameters()` 中：

```java
// 提取所有 Parameter 节点（按位置排序）
List<Parameter> parametersList = extractParameters(statement);

// 创建 parameterLookup 映射
ImmutableMap.Builder<NodeRef<Parameter>, Expression> builder = ImmutableMap.builder();
Iterator<Literal> iterator = values.iterator();
for (Parameter parameter : parametersList) {
  builder.put(NodeRef.of(parameter), iterator.next());  // 使用 NodeRef 作为 key
}
return builder.buildOrThrow();
```

在 `ExpressionAnalyzer.visitParameter()` 中使用：

```java
Expression providedValue = parameters.get(NodeRef.of(node));  // 使用 NodeRef 查找
```

---

## 总结

1. **Parameter 创建位置**：在 `AstBuilder` 的 `visitParameter()`、`visitLimit()`、`visitOffset()` 方法中创建，使用 `parameterPosition` 作为 `id`。

2. **id 字段含义**：表示参数在 SQL 中出现的顺序位置（从 0 开始），用于标识参数的位置。

3. **使用 NodeRef 的原因**：
   - `Parameter` 的 `equals()` 基于 `id`，可能导致不同对象实例被认为是相等的
   - `NodeRef` 使用对象引用作为标识，确保每个节点实例的唯一性
   - 保证参数绑定的准确性和安全性

**设计原则**：使用 `NodeRef` 是 IoTDB（和 Trino）的标准做法，确保 AST 节点在 Map 中的唯一性和准确性。




