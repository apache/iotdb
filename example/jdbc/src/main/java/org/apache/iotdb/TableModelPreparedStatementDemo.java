/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

/**
 * 表模型 JDBC PreparedStatement 示例程序
 *
 * <p>本示例展示了如何使用 JDBC PreparedStatement 来操作 IoTDB 表模型，包括：
 *
 * <ul>
 *   <li>创建数据库和表
 *   <li>使用 PreparedStatement 插入数据
 *   <li>使用 PreparedStatement 查询数据（各种参数类型）
 *   <li>批量插入数据
 *   <li>聚合查询
 * </ul>
 *
 * <p>运行前请确保 IoTDB 服务已启动在 127.0.0.1:6667
 */
public class TableModelPreparedStatementDemo {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TableModelPreparedStatementDemo.class);

  // IoTDB 连接配置
  private static final String JDBC_URL = "jdbc:iotdb://127.0.0.1:6667?sql_dialect=table";
  private static final String USERNAME = "root";
  private static final String PASSWORD = "root";
  private static final String DATABASE_NAME = "demo_db";

  public static void main(String[] args) {
    try {
      // 加载 JDBC 驱动
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      LOGGER.info("JDBC 驱动加载成功");

      // 跳过数据库创建和插入，直接测试查询
      // 1. 创建数据库和表
      // setupDatabaseAndTables();

      // 2. 使用 PreparedStatement 插入数据
      // insertDataWithPreparedStatement();

      // 3. 批量插入数据
      // batchInsertData();

      // 3.5 使用普通 Statement 验证数据是否存在
      // verifyDataWithStatement();

      // 4. 使用不同参数类型查询
      queryWithIntParameter();
      queryWithStringParameter();
      queryWithDoubleParameter();
      queryWithMultipleParameters();

      // 5. 聚合查询
      aggregationQuery();

      // 6. 演示参数清除和重复执行
      demonstrateParameterReuseAndClear();

      LOGGER.info("所有示例执行完成！");

    } catch (ClassNotFoundException e) {
      LOGGER.error("找不到 IoTDB JDBC 驱动", e);
    } catch (SQLException e) {
      LOGGER.error("SQL 执行错误", e);
    }
  }

  /** 创建数据库和表 */
  private static void setupDatabaseAndTables() throws SQLException {
    LOGGER.info("=== 创建数据库和表 ===");

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        Statement statement = connection.createStatement()) {

      // 创建数据库（如果已存在则忽略）
      try {
        statement.execute("CREATE DATABASE " + DATABASE_NAME);
        LOGGER.info("数据库 {} 创建成功", DATABASE_NAME);
      } catch (SQLException e) {
        LOGGER.info("数据库 {} 已存在，继续使用", DATABASE_NAME);
      }

      // 切换到数据库
      statement.execute("USE " + DATABASE_NAME);

      // 创建设备表 - 包含各种数据类型（如果已存在则忽略）
      try {
        statement.execute(
            "CREATE TABLE device_data("
                + "region_id STRING TAG, "
                + "device_id STRING TAG, "
                + "device_name STRING ATTRIBUTE, "
                + "temperature FLOAT FIELD, "
                + "humidity DOUBLE FIELD, "
                + "status INT32 FIELD, "
                + "error_code INT64 FIELD, "
                + "is_online BOOLEAN FIELD"
                + ")");
        LOGGER.info("表 device_data 创建成功");
      } catch (SQLException e) {
        LOGGER.info("表 device_data 已存在，继续使用");
      }

      // 创建用户表（如果已存在则忽略）
      try {
        statement.execute(
            "CREATE TABLE user_info("
                + "user_id INT32 FIELD, "
                + "username STRING FIELD, "
                + "score DOUBLE FIELD"
                + ")");
        LOGGER.info("表 user_info 创建成功");
      } catch (SQLException e) {
        LOGGER.info("表 user_info 已存在，继续使用");
      }
    }
  }

  /** 使用 PreparedStatement 插入单条数据 */
  private static void insertDataWithPreparedStatement() throws SQLException {
    LOGGER.info("=== 使用 PreparedStatement 插入数据 ===");

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        Statement stmt = connection.createStatement()) {

      stmt.execute("USE " + DATABASE_NAME);

      // 使用 PreparedStatement 插入数据
      String insertSql =
          "INSERT INTO device_data(time, region_id, device_id, device_name, "
              + "temperature, humidity, status, error_code, is_online) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

      try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
        // 插入第一条数据
        ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        ps.setString(2, "north");
        ps.setString(3, "device001");
        ps.setString(4, "温度传感器A");
        ps.setFloat(5, 25.5f);
        ps.setDouble(6, 60.0);
        ps.setInt(7, 1);
        ps.setLong(8, 0L);
        ps.setBoolean(9, true);
        ps.executeUpdate();
        LOGGER.info("插入数据: region=north, device=device001");

        // 插入第二条数据
        ps.setTimestamp(1, new Timestamp(System.currentTimeMillis() + 1000));
        ps.setString(2, "north");
        ps.setString(3, "device002");
        ps.setString(4, "温度传感器B");
        ps.setFloat(5, 26.3f);
        ps.setDouble(6, 55.5);
        ps.setInt(7, 1);
        ps.setLong(8, 0L);
        ps.setBoolean(9, true);
        ps.executeUpdate();
        LOGGER.info("插入数据: region=north, device=device002");

        // 插入第三条数据
        ps.setTimestamp(1, new Timestamp(System.currentTimeMillis() + 2000));
        ps.setString(2, "south");
        ps.setString(3, "device003");
        ps.setString(4, "湿度传感器");
        ps.setFloat(5, 30.0f);
        ps.setDouble(6, 70.2);
        ps.setInt(7, 0);
        ps.setLong(8, 101L);
        ps.setBoolean(9, false);
        ps.executeUpdate();
        LOGGER.info("插入数据: region=south, device=device003");
      }
    }
  }

  /** 批量插入数据 */
  private static void batchInsertData() throws SQLException {
    LOGGER.info("=== 批量插入数据 ===");

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        Statement stmt = connection.createStatement()) {

      stmt.execute("USE " + DATABASE_NAME);

      String insertSql =
          "INSERT INTO user_info(time, user_id, username, score) VALUES (?, ?, ?, ?)";

      try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
        long baseTime = System.currentTimeMillis();

        // 批量添加数据
        for (int i = 1; i <= 5; i++) {
          ps.setTimestamp(1, new Timestamp(baseTime + i * 1000));
          ps.setInt(2, i);
          ps.setString(3, "user_" + i);
          ps.setDouble(4, 80.0 + i * 2.5);
          ps.addBatch();
        }

        // 执行批量插入
        int[] results = ps.executeBatch();
        LOGGER.info("批量插入完成，影响行数: {}", results.length);
      }

      // 验证插入结果
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM user_info")) {
        if (rs.next()) {
          LOGGER.info("user_info 表当前记录数: {}", rs.getLong("cnt"));
        }
      }
    }
  }

  /** 使用普通 Statement 验证数据是否存在 */
  private static void verifyDataWithStatement() throws SQLException {
    LOGGER.info("=== 使用普通 Statement 验证数据 ===");

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        Statement stmt = connection.createStatement()) {

      stmt.execute("USE " + DATABASE_NAME);

      // 查询 device_data 表
      LOGGER.info("--- 使用 Statement 查询 device_data 表 ---");
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM device_data")) {
        printResultSet(rs, "device_data 全部数据");
      }

      // 查询 user_info 表
      LOGGER.info("--- 使用 Statement 查询 user_info 表 ---");
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM user_info")) {
        printResultSet(rs, "user_info 全部数据");
      }

      // 使用 Statement 带条件查询
      LOGGER.info("--- 使用 Statement 带条件查询 ---");
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM user_info WHERE user_id = 3")) {
        printResultSet(rs, "Statement 查询 user_id = 3");
      }
    }
  }

  /** 使用 INT 参数查询 */
  private static void queryWithIntParameter() throws SQLException {
    LOGGER.info("=== 使用 INT 参数查询 ===");

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        Statement stmt = connection.createStatement()) {

      stmt.execute("USE " + DATABASE_NAME);

      // 测试1: 使用 PreparedStatement 查询
      String querySql = "SELECT * FROM user_info WHERE user_id = ?";
      LOGGER.info("执行 PreparedStatement: {}", querySql);
      try (PreparedStatement ps = connection.prepareStatement(querySql)) {
        // 打印 PreparedStatement 的元数据
        LOGGER.info("PreparedStatement 类型: {}", ps.getClass().getName());
        LOGGER.info("参数数量: {}", ps.getParameterMetaData().getParameterCount());

        ps.setInt(1, 3);
        LOGGER.info("设置参数 1 = 3 (类型: INT)");

        try (ResultSet rs = ps.executeQuery()) {
          LOGGER.info("executeQuery 返回，开始遍历结果");
          printResultSet(rs, "PreparedStatement 查询 user_id = 3 的结果");
        }
      }

      // 测试2: 尝试使用 setLong 代替 setInt
      LOGGER.info("--- 尝试使用 setLong 代替 setInt ---");
      try (PreparedStatement ps = connection.prepareStatement(querySql)) {
        ps.setLong(1, 3L);
        LOGGER.info("设置参数 1 = 3L (类型: LONG)");
        try (ResultSet rs = ps.executeQuery()) {
          printResultSet(rs, "PreparedStatement (setLong) 查询 user_id = 3 的结果");
        }
      }

      // 测试3: 使用普通 Statement 查询相同的条件
      LOGGER.info("--- 对比：使用普通 Statement 查询相同条件 ---");
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM user_info WHERE user_id = 3")) {
        printResultSet(rs, "Statement 查询 user_id = 3 的结果");
      }

      // 测试4: 使用 Statement 模拟 CLI 的 PREPARE/EXECUTE 方式
      LOGGER.info("--- 测试 CLI 风格的 PREPARE/EXECUTE ---");
      try {
        stmt.execute("PREPARE cli_stmt FROM SELECT * FROM user_info WHERE user_id = ?");
        LOGGER.info("PREPARE 成功");
        try (ResultSet rs = stmt.executeQuery("EXECUTE cli_stmt USING 3")) {
          printResultSet(rs, "CLI风格 EXECUTE 查询 user_id = 3 的结果");
        }
        stmt.execute("DEALLOCATE PREPARE cli_stmt");
        LOGGER.info("DEALLOCATE 成功");
      } catch (SQLException e) {
        LOGGER.error("CLI 风格 PREPARE/EXECUTE 失败: {}", e.getMessage());
      }

      // 测试5: 关键测试 - 使用 JDBC PreparedStatement 进行 PREPARE，然后用 SQL EXECUTE 执行
      LOGGER.info("--- 测试5: JDBC PreparedStatement PREPARE + SQL EXECUTE ---");
      String testStmtName = null;
      try (PreparedStatement ps = connection.prepareStatement(querySql)) {
        // 获取 JDBC PreparedStatement 内部的 statementName
        // 通过反射或者检查 SHOW PREPARED STATEMENTS
        LOGGER.info("创建 PreparedStatement 成功");

        // 先用 JDBC PreparedStatement 执行一次（对比）
        ps.setInt(1, 3);
        try (ResultSet rs = ps.executeQuery()) {
          printResultSet(rs, "测试5: JDBC PreparedStatement 执行结果");
        }
      }

      // 测试6: 使用 SHOW 命令查看当前会话的 prepared statements
      LOGGER.info("--- 测试6: 查看会话中的 Prepared Statements ---");
      try (ResultSet rs = stmt.executeQuery("SHOW PREPARED STATEMENTS")) {
        printResultSet(rs, "当前会话的 Prepared Statements");
      } catch (SQLException e) {
        LOGGER.info("SHOW PREPARED STATEMENTS 不支持或执行失败: {}", e.getMessage());
      }
    }
  }

  /** 使用 STRING 参数查询 */
  private static void queryWithStringParameter() throws SQLException {
    LOGGER.info("=== 使用 STRING 参数查询 ===");

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        Statement stmt = connection.createStatement()) {

      stmt.execute("USE " + DATABASE_NAME);

      String querySql = "SELECT * FROM device_data WHERE region_id = ?";
      try (PreparedStatement ps = connection.prepareStatement(querySql)) {
        ps.setString(1, "north");
        try (ResultSet rs = ps.executeQuery()) {
          printResultSet(rs, "查询 region_id = 'north' 的结果");
        }
      }
    }
  }

  /** 使用 DOUBLE 参数查询 */
  private static void queryWithDoubleParameter() throws SQLException {
    LOGGER.info("=== 使用 DOUBLE 参数查询 ===");

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        Statement stmt = connection.createStatement()) {

      stmt.execute("USE " + DATABASE_NAME);

      String querySql = "SELECT * FROM device_data WHERE temperature > ?";
      try (PreparedStatement ps = connection.prepareStatement(querySql)) {
        ps.setDouble(1, 26.0);
        try (ResultSet rs = ps.executeQuery()) {
          printResultSet(rs, "查询 temperature > 26.0 的结果");
        }
      }
    }
  }

  /** 使用多个参数查询 */
  private static void queryWithMultipleParameters() throws SQLException {
    LOGGER.info("=== 使用多个参数查询 ===");

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        Statement stmt = connection.createStatement()) {

      stmt.execute("USE " + DATABASE_NAME);

      String querySql =
          "SELECT * FROM device_data WHERE region_id = ? AND temperature >= ? AND is_online = ?";
      try (PreparedStatement ps = connection.prepareStatement(querySql)) {
        ps.setString(1, "north");
        ps.setFloat(2, 25.0f);
        ps.setBoolean(3, true);
        try (ResultSet rs = ps.executeQuery()) {
          printResultSet(rs, "查询 region='north' AND temperature>=25 AND is_online=true 的结果");
        }
      }
    }
  }

  /** 聚合查询 */
  private static void aggregationQuery() throws SQLException {
    LOGGER.info("=== 聚合查询 ===");

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        Statement stmt = connection.createStatement()) {

      stmt.execute("USE " + DATABASE_NAME);

      // 带参数的聚合查询
      String querySql =
          "SELECT COUNT(*) as cnt, AVG(temperature) as avg_temp, MAX(humidity) as max_humidity "
              + "FROM device_data WHERE region_id = ?";
      try (PreparedStatement ps = connection.prepareStatement(querySql)) {
        ps.setString(1, "north");
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            LOGGER.info(
                "北区统计 - 设备数: {}, 平均温度: {}, 最大湿度: {}",
                rs.getLong("cnt"),
                rs.getDouble("avg_temp"),
                rs.getDouble("max_humidity"));
          }
        }
      }

      // 分数范围查询
      String scoreSql = "SELECT * FROM user_info WHERE score BETWEEN ? AND ?";
      try (PreparedStatement ps = connection.prepareStatement(scoreSql)) {
        ps.setDouble(1, 85.0);
        ps.setDouble(2, 95.0);
        try (ResultSet rs = ps.executeQuery()) {
          printResultSet(rs, "查询分数在 85-95 之间的用户");
        }
      }
    }
  }

  /** 演示参数清除和重复执行 */
  private static void demonstrateParameterReuseAndClear() throws SQLException {
    LOGGER.info("=== 参数清除和重复执行 ===");

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        Statement stmt = connection.createStatement()) {

      stmt.execute("USE " + DATABASE_NAME);

      String querySql = "SELECT * FROM user_info WHERE user_id = ?";
      try (PreparedStatement ps = connection.prepareStatement(querySql)) {
        // 第一次查询
        ps.setInt(1, 1);
        try (ResultSet rs = ps.executeQuery()) {
          printResultSet(rs, "第一次查询: user_id = 1");
        }

        // 清除参数
        ps.clearParameters();

        // 第二次查询 - 使用不同参数
        ps.setInt(1, 5);
        try (ResultSet rs = ps.executeQuery()) {
          printResultSet(rs, "第二次查询: user_id = 5");
        }

        // 直接设置新参数（不清除也可以）
        ps.setInt(1, 3);
        try (ResultSet rs = ps.executeQuery()) {
          printResultSet(rs, "第三次查询: user_id = 3");
        }
      }
    }
  }

  /** 打印 ResultSet 结果 */
  private static void printResultSet(ResultSet rs, String title) throws SQLException {
    LOGGER.info("--- {} ---", title);

    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();

    // 打印列名
    StringBuilder header = new StringBuilder();
    for (int i = 1; i <= columnCount; i++) {
      header.append(metaData.getColumnName(i));
      if (i < columnCount) {
        header.append(" | ");
      }
    }
    LOGGER.info("列名: {}", header);

    // 打印数据行
    int rowCount = 0;
    while (rs.next()) {
      StringBuilder row = new StringBuilder();
      for (int i = 1; i <= columnCount; i++) {
        Object value = rs.getObject(i);
        row.append(value != null ? value.toString() : "null");
        if (i < columnCount) {
          row.append(" | ");
        }
      }
      LOGGER.info("  {}", row);
      rowCount++;
    }
    LOGGER.info("共 {} 条记录", rowCount);
  }
}
