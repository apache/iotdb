package org.apache.iotdb.session.examples;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSession;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

/**
 * 示例：演示 PreparedStatement 中字符串转义的使用
 * 
 * 当参数值包含单引号时，ParameterFormatter 会自动转义
 * 转义规则：单引号通过加倍来转义（' -> ''）
 */
public class PreparedStatementStringEscapeExample {

  public static void main(String[] args) {
    // 创建 Session 和 TableSession
    Session session = new Session("127.0.0.1", 6667, "root", "root");
    ITableSession tableSession = null;

    try {
      session.open();
      tableSession = new TableSession(session);

      // ========== 示例 1：用户名称包含单引号 ==========
      System.out.println("=== 示例 1：查找用户名为 O'Brien 的记录 ===");
      
      // 准备查询语句
      tableSession.prepare("findUser", "SELECT * FROM users WHERE name = ?");
      
      // 执行查询 - 参数值 "O'Brien" 包含单引号，会自动转义为 'O''Brien'
      // 实际执行的 SQL: EXECUTE findUser USING 'O''Brien'
      SessionDataSet result = tableSession.execute("findUser", "O'Brien");
      
      System.out.println("查询结果：");
      while (result.hasNext()) {
        System.out.println(result.next());
      }
      
      tableSession.deallocate("findUser");
      System.out.println();

      // ========== 示例 2：产品描述包含多个单引号 ==========
      System.out.println("=== 示例 2：插入包含单引号的产品描述 ===");
      
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
      
      System.out.println("插入成功：Product A - It's a 'premium' product");
      
      // 再次执行，使用不同的描述
      // 参数值: "Don't miss this 'special' offer!"
      // 实际执行的 SQL: EXECUTE insertProduct USING 2, 'Product B', 'Don''t miss this ''special'' offer!'
      tableSession.execute("insertProduct", 
          2, 
          "Product B", 
          "Don't miss this 'special' offer!");
      
      System.out.println("插入成功：Product B - Don't miss this 'special' offer!");
      
      tableSession.deallocate("insertProduct");
      System.out.println();

      // ========== 示例 3：使用 EXECUTE IMMEDIATE ==========
      System.out.println("=== 示例 3：立即执行查询（不需要 PREPARE）===");
      
      // 立即执行，参数值包含单引号：'John's car'
      // 实际执行的 SQL: EXECUTE IMMEDIATE 'SELECT * FROM vehicles WHERE owner = ?' USING 'John''s car'
      SessionDataSet result2 = tableSession.executeImmediate(
          "SELECT * FROM vehicles WHERE owner = ?", 
          "John's car");
      
      System.out.println("查询结果：");
      while (result2.hasNext()) {
        System.out.println(result2.next());
      }
      System.out.println();

      // ========== 示例 4：复杂查询场景 ==========
      System.out.println("=== 示例 4：查找包含特定文本的评论 ===");
      
      // 准备复杂查询：查找包含特定文本的评论
      tableSession.prepare("findComments", 
          "SELECT * FROM comments WHERE content LIKE ? AND author = ?");
      
      // 执行查询
      // 参数值: "%It's great%" 和 "Alice O'Brien"
      // 实际执行的 SQL: EXECUTE findComments USING '%It''s great%', 'Alice O''Brien'
      SessionDataSet result3 = tableSession.execute("findComments", 
          "%It's great%", 
          "Alice O'Brien");
      
      System.out.println("查询结果：");
      while (result3.hasNext()) {
        System.out.println(result3.next());
      }
      
      tableSession.deallocate("findComments");
      System.out.println();

      // ========== 示例 5：展示转义效果 ==========
      System.out.println("=== 示例 5：转义效果对比 ===");
      
      String[] testStrings = {
          "test",           // 普通字符串
          "It's",           // 包含一个单引号
          "Don't",          // 包含一个单引号
          "'quoted'",       // 包含两个单引号
          "It's a 'test'",  // 包含多个单引号
          "O'Brien"         // 人名中的单引号
      };
      
      System.out.println("输入值 -> SQL 字面量：");
      for (String testStr : testStrings) {
        // 模拟 ParameterFormatter.formatParameter() 的逻辑
        String escaped = testStr.replace("'", "''");
        String sqlLiteral = "'" + escaped + "'";
        System.out.println("  \"" + testStr + "\" -> " + sqlLiteral);
      }

    } catch (StatementExecutionException | IoTDBConnectionException e) {
      System.err.println("执行错误: " + e.getMessage());
      e.printStackTrace();
    } finally {
      try {
        if (tableSession != null) {
          tableSession.close();
        }
        if (session != null) {
          session.close();
        }
      } catch (IoTDBConnectionException e) {
        System.err.println("关闭连接错误: " + e.getMessage());
      }
    }
  }
}




