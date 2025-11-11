# TableSession PreparedStatement 字符串转义示例

## 字符串转义场景

当参数值包含单引号（`'`）时，需要进行转义。`ParameterFormatter` 会自动处理这种情况，将单引号加倍（SQL 标准做法）。

## 转义规则

- **输入**：`"It's a test"`
- **输出**：`'It''s a test'`（单引号被加倍）

## 完整使用示例

### 示例 1：用户名称包含单引号

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
    // 准备查询语句：查找用户名为 "O'Brien" 的记录
    tableSession.prepare("findUser", 
        "SELECT * FROM users WHERE name = ?");
    
    // 执行查询 - 参数值 "O'Brien" 包含单引号，会自动转义
    // 实际执行的 SQL: EXECUTE findUser USING 'O''Brien'
    SessionDataSet result = tableSession.execute("findUser", "O'Brien");
    
    // 处理结果...
    while (result.hasNext()) {
        System.out.println(result.next());
    }
    
    tableSession.deallocate("findUser");
    
} catch (StatementExecutionException | IoTDBConnectionException e) {
    e.printStackTrace();
} finally {
    tableSession.close();
}
```

### 示例 2：描述字段包含多个单引号

```java
try {
    // 准备插入语句
    tableSession.prepare("insertProduct", 
        "INSERT INTO products (id, name, description) VALUES (?, ?, ?)");
    
    // 执行插入 - description 包含多个单引号
    // 参数值: "It's a 'premium' product"
    // 实际执行的 SQL: EXECUTE insertProduct USING 1, 'Product A', 'It''s a ''premium'' product'
    tableSession.execute("insertProduct", 
        1, 
        "Product A", 
        "It's a 'premium' product");
    
    // 再次执行，使用不同的描述
    // 参数值: "Don't miss this 'special' offer!"
    // 实际执行的 SQL: EXECUTE insertProduct USING 2, 'Product B', 'Don''t miss this ''special'' offer!'
    tableSession.execute("insertProduct", 
        2, 
        "Product B", 
        "Don't miss this 'special' offer!");
    
    tableSession.deallocate("insertProduct");
    
} catch (StatementExecutionException | IoTDBConnectionException e) {
    e.printStackTrace();
}
```

### 示例 3：使用 EXECUTE IMMEDIATE

```java
try {
    // 立即执行，不需要 PREPARE
    // 参数值包含单引号：'John's car'
    // 实际执行的 SQL: EXECUTE IMMEDIATE 'SELECT * FROM vehicles WHERE owner = ?' USING 'John''s car'
    SessionDataSet result = tableSession.executeImmediate(
        "SELECT * FROM vehicles WHERE owner = ?", 
        "John's car");
    
    // 处理结果...
    while (result.hasNext()) {
        System.out.println(result.next());
    }
    
} catch (StatementExecutionException | IoTDBConnectionException e) {
    e.printStackTrace();
}
```

### 示例 4：复杂查询场景

```java
try {
    // 准备复杂查询：查找包含特定文本的评论
    tableSession.prepare("findComments", 
        "SELECT * FROM comments WHERE content LIKE ? AND author = ?");
    
    // 执行查询
    // 参数值: "%It's great%" 和 "Alice O'Brien"
    // 实际执行的 SQL: 
    //   EXECUTE findComments USING '%It''s great%', 'Alice O''Brien'
    SessionDataSet result = tableSession.execute("findComments", 
        "%It's great%", 
        "Alice O'Brien");
    
    // 处理结果...
    while (result.hasNext()) {
        System.out.println(result.next());
    }
    
    tableSession.deallocate("findComments");
    
} catch (StatementExecutionException | IoTDBConnectionException e) {
    e.printStackTrace();
}
```

## 转义过程说明

### 手动构建 SQL（容易出错）

```java
// ❌ 错误做法：手动拼接 SQL，容易出错
String name = "O'Brien";
String sql = "SELECT * FROM users WHERE name = '" + name + "'";
// 结果: SELECT * FROM users WHERE name = 'O'Brien'
// 这会导致 SQL 语法错误！

// ✅ 正确做法：手动转义（繁琐且容易遗漏）
String name = "O'Brien";
String escapedName = name.replace("'", "''");
String sql = "SELECT * FROM users WHERE name = '" + escapedName + "'";
// 结果: SELECT * FROM users WHERE name = 'O''Brien'
```

### 使用 PreparedStatement（自动转义）

```java
// ✅ 推荐做法：使用 PreparedStatement，自动处理转义
tableSession.prepare("findUser", "SELECT * FROM users WHERE name = ?");
tableSession.execute("findUser", "O'Brien");
// ParameterFormatter 自动将 "O'Brien" 转换为 'O''Brien'
// 无需手动处理转义，更安全、更简单
```

## 转义示例对照表

| 输入值 | 转义后的 SQL 字面量 | 说明 |
|--------|-------------------|------|
| `"test"` | `'test'` | 普通字符串 |
| `"It's"` | `'It''s'` | 包含一个单引号 |
| `"Don't"` | `'Don''t'` | 包含一个单引号 |
| `"'quoted'"` | `'''quoted'''` | 包含两个单引号 |
| `"It's a 'test'"` | `'It''s a ''test'''` | 包含多个单引号 |
| `"O'Brien"` | `'O''Brien'` | 人名中的单引号 |
| `null` | `NULL` | null 值 |

## 安全性优势

使用 PreparedStatement 自动转义可以：

1. **防止 SQL 注入**：参数值被正确转义，无法注入恶意 SQL
2. **简化代码**：无需手动处理转义逻辑
3. **减少错误**：避免忘记转义导致的 SQL 语法错误
4. **提高可读性**：代码更清晰，意图更明确

## 注意事项

1. **单引号转义**：单引号通过加倍来转义（`'` → `''`），这是 SQL 标准做法
2. **其他特殊字符**：目前只处理单引号，其他特殊字符（如反斜杠）可能需要根据数据库的具体实现来处理
3. **NULL 值**：`null` 参数会被转换为 `NULL`（不带引号）
4. **数字和布尔值**：数字和布尔值不需要引号，直接使用

## 测试建议

建议测试以下场景：
- 普通字符串（无特殊字符）
- 包含单个单引号的字符串
- 包含多个单引号的字符串
- 以单引号开头或结尾的字符串
- 只包含单引号的字符串（`"'"`）
- null 值
- 空字符串（`""`）




