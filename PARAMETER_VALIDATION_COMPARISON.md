# 参数校验对比：IoTDB vs Trino

## 问题

IoTDB 的 `Coordinator.java` 中缺少对参数的显式校验，而 Trino 的 `QueryPreparer.java` 中有完整的参数校验。

---

## Trino 的实现

### QueryPreparer.validateParameters()

**文件**：`trino-main/src/main/java/io/trino/execution/QueryPreparer.java`

```java:92:101:trino-main/src/main/java/io/trino/execution/QueryPreparer.java
private static void validateParameters(Statement node, List<Expression> parameterValues)
{
    int parameterCount = getParameterCount(node);
    if (parameterValues.size() != parameterCount) {
        throw semanticException(INVALID_PARAMETER_USAGE, node, "Incorrect number of parameters: expected %s but found %s", parameterCount, parameterValues.size());
    }
    for (Expression expression : parameterValues) {
        verifyExpressionIsConstant(emptySet(), expression);
    }
}
```

**校验内容**：
1. ✅ **参数数量检查**：`getParameterCount(node)` vs `parameterValues.size()`
2. ✅ **常量表达式验证**：`verifyExpressionIsConstant()` - 确保参数是常量，不能包含列引用等

---

## IoTDB 的当前实现

### 语法限制

IoTDB 的语法已经限制了参数必须是 `literalExpression`（字面量表达式）：

```antlr
executeStatement
    : EXECUTE statementName=identifier (USING literalExpression (',' literalExpression)*)?
    ;

executeImmediateStatement
    : EXECUTE IMMEDIATE sql=string (USING literalExpression (',' literalExpression)*)?
    ;
```

### AST 节点类型

- `Execute.parameters`: `List<Literal>` - 类型已经是 `Literal`
- `ExecuteImmediate.parameters`: `List<Literal>` - 类型已经是 `Literal`

### 当前校验

- ✅ **参数数量检查**：在 `ParameterExtractor.bindParameters()` 中检查
- ❌ **缺少显式验证**：没有在 `Coordinator` 中提前验证参数

---

## 改进方案

### 1. 添加 `validateParameters()` 方法

**文件**：`ParameterExtractor.java`

```java
/**
 * Validate parameters before binding. Checks parameter count and ensures all parameters are
 * literals (constants).
 *
 * @param statement the statement containing Parameter nodes
 * @param values the parameter values (in order)
 * @throws SemanticException if validation fails
 */
public static void validateParameters(Statement statement, List<Literal> values) {
  int parameterCount = getParameterCount(statement);
  if (values.size() != parameterCount) {
    throw new SemanticException(
        String.format(
            "Invalid number of parameters: expected %d, got %d", parameterCount, values.size()));
  }
  // Note: In IoTDB, parameters are already restricted to Literal by grammar (literalExpression),
  // so we don't need to verify that they are constants like Trino does.
  // However, we still validate the count here for clarity and early error detection.
}
```

### 2. 在 `Coordinator` 中调用验证

**文件**：`Coordinator.java`

```java
// EXECUTE 处理
if (statement instanceof Execute) {
    // ...
    // 3. Validate parameters (check count, similar to Trino's QueryPreparer.validateParameters)
    ParameterExtractor.validateParameters(cachedStatement, executeStatement.getParameters());
    
    // 4. Bind parameters
    Map<NodeRef<Parameter>, Expression> parameterLookup =
        ParameterExtractor.bindParameters(cachedStatement, executeStatement.getParameters());
    // ...
}

// EXECUTE IMMEDIATE 处理
else if (statement instanceof ExecuteImmediate) {
    // ...
    if (!parameters.isEmpty()) {
        // Validate parameters (check count, similar to Trino's QueryPreparer.validateParameters)
        ParameterExtractor.validateParameters(parsedStatement, parameters);
        parameterLookup = ParameterExtractor.bindParameters(parsedStatement, parameters);
        // ...
    }
}
```

---

## 为什么需要显式验证？

### 1. **早期错误检测**
- 在绑定参数之前就发现错误，提供更清晰的错误信息
- 与 Trino 的实现保持一致

### 2. **代码清晰性**
- 明确表达"验证参数"的意图
- 便于后续扩展（如果需要添加更多验证）

### 3. **安全性**
- 虽然语法已经限制了参数类型，但显式验证提供了额外的安全层
- 如果将来语法发生变化，验证逻辑仍然有效

---

## 与 Trino 的差异

| 特性 | Trino | IoTDB |
|------|-------|-------|
| 参数类型 | `List<Expression>` | `List<Literal>` |
| 语法限制 | 无（允许任意表达式） | `literalExpression`（只允许字面量） |
| 常量验证 | ✅ `verifyExpressionIsConstant()` | ❌ 不需要（语法已限制） |
| 数量验证 | ✅ `validateParameters()` | ✅ `validateParameters()`（新增） |

**结论**：
- IoTDB 的语法已经限制了参数必须是 `Literal`，所以不需要像 Trino 那样验证常量表达式
- 但应该添加显式的参数数量验证，与 Trino 保持一致

---

## 实现后的效果

### 之前（缺少显式验证）：
```java
// 直接绑定参数，错误在 bindParameters 内部抛出
Map<NodeRef<Parameter>, Expression> parameterLookup =
    ParameterExtractor.bindParameters(cachedStatement, executeStatement.getParameters());
```

### 之后（添加显式验证）：
```java
// 1. 显式验证参数（早期错误检测）
ParameterExtractor.validateParameters(cachedStatement, executeStatement.getParameters());

// 2. 绑定参数
Map<NodeRef<Parameter>, Expression> parameterLookup =
    ParameterExtractor.bindParameters(cachedStatement, executeStatement.getParameters());
```

**优势**：
- ✅ 代码意图更清晰
- ✅ 错误信息更早出现
- ✅ 与 Trino 的实现保持一致
- ✅ 便于后续扩展验证逻辑




