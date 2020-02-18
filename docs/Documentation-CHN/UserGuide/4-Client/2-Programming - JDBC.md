<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# 第4章: 客户端

## 编程 - JDBC
## 使用

### 依赖项

* JDK >= 1.8
* Maven >= 3.1

### 只打包 JDBC 工程

在根目录下执行下面的命令:
```
mvn clean package -pl jdbc -am -Dmaven.test.skip=true
```

### 如何到本地 MAVEN 仓库

在根目录下执行下面的命令:
```
mvn clean install -pl jdbc -am -Dmaven.test.skip=true
```

### 如何在 MAVEN 中使用 IoTDB JDBC

```
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-jdbc</artifactId>
      <version>0.8.0</version>
    </dependency>
</dependencies>
```

### 示例

本章提供了如何建立数据库连接、执行 SQL 和显示查询结果的示例。

要求您已经在工程中包含了数据库编程所需引入的包和 JDBC class.

**注意:为了更快地插入，建议使用 insertBatch()**

```Java
import java.sql.*;
import org.apache.iotdb.jdbc.IoTDBSQLException;

public class JDBCExample {
  /**
   * Before executing a SQL statement with a Statement object, you need to create a Statement object using the createStatement() method of the Connection object.
   * After creating a Statement object, you can use its execute() method to execute a SQL statement
   * Finally, remember to close the 'statement' and 'connection' objects by using their close() method
   * For statements with query results, we can use the getResultSet() method of the Statement object to get the result set.
   */
  public static void main(String[] args) throws SQLException {
    Connection connection = getConnection();
    if (connection == null) {
      System.out.println("get connection defeat");
      return;
    }
    Statement statement = connection.createStatement();
    //Create storage group
    try {
      statement.execute("SET STORAGE GROUP TO root.demo");
    }catch (IoTDBSQLException e){
      System.out.println(e.getMessage());
    }


    //Show storage group
    statement.execute("SHOW STORAGE GROUP");
    outputResult(statement.getResultSet());

    //Create time series
    //Different data type has different encoding methods. Here use INT32 as an example
    try {
      statement.execute("CREATE TIMESERIES root.demo.s0 WITH DATATYPE=INT32,ENCODING=RLE;");
    }catch (IoTDBSQLException e){
      System.out.println(e.getMessage());
    }
    //Show time series
    statement.execute("SHOW TIMESERIES root.demo");
    outputResult(statement.getResultSet());
    //Show devices
    statement.execute("SHOW DEVICES");
    outputResult(statement.getResultSet());
    //Count time series
    statement.execute("COUNT TIMESERIES root");
    outputResult(statement.getResultSet());
    //Count nodes at the given level
    statement.execute("COUNT NODES root LEVEL=3");
    outputResult(statement.getResultSet());
    //Count timeseries group by each node at the given level
    statement.execute("COUNT TIMESERIES root GROUP BY LEVEL=3");
    outputResult(statement.getResultSet());
    

    //Execute insert statements in batch
    statement.addBatch("insert into root.demo(timestamp,s0) values(1,1);");
    statement.addBatch("insert into root.demo(timestamp,s0) values(1,1);");
    statement.addBatch("insert into root.demo(timestamp,s0) values(2,15);");
    statement.addBatch("insert into root.demo(timestamp,s0) values(2,17);");
    statement.addBatch("insert into root.demo(timestamp,s0) values(4,12);");
    statement.executeBatch();
    statement.clearBatch();

    //Full query statement
    String sql = "select * from root.demo";
    ResultSet resultSet = statement.executeQuery(sql);
    System.out.println("sql: " + sql);
    outputResult(resultSet);

    //Exact query statement
    sql = "select s0 from root.demo where time = 4;";
    resultSet= statement.executeQuery(sql);
    System.out.println("sql: " + sql);
    outputResult(resultSet);

    //Time range query
    sql = "select s0 from root.demo where time >= 2 and time < 5;";
    resultSet = statement.executeQuery(sql);
    System.out.println("sql: " + sql);
    outputResult(resultSet);

    //Aggregate query
    sql = "select count(s0) from root.demo;";
    resultSet = statement.executeQuery(sql);
    System.out.println("sql: " + sql);
    outputResult(resultSet);

    //Delete time series
    statement.execute("delete timeseries root.demo.s0");

    //close connection
    statement.close();
    connection.close();
  }

  public static Connection getConnection() {
    // JDBC driver name and database URL
    String driver = "org.apache.iotdb.jdbc.IoTDBDriver";
    String url = "jdbc:iotdb://127.0.0.1:6667/";

    // Database credentials
    String username = "root";
    String password = "root";

    Connection connection = null;
    try {
      Class.forName(driver);
      connection = DriverManager.getConnection(url, username, password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return connection;
  }

  /**
   * This is an example of outputting the results in the ResultSet
   */
  private static void outputResult(ResultSet resultSet) throws SQLException {
    if (resultSet != null) {
      System.out.println("--------------------------");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int columnCount = metaData.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        System.out.print(metaData.getColumnLabel(i + 1) + " ");
      }
      System.out.println();
      while (resultSet.next()) {
        for (int i = 1; ; i++) {
          System.out.print(resultSet.getString(i));
          if (i < columnCount) {
            System.out.print(", ");
          } else {
            System.out.println();
            break;
          }
        }
      }
      System.out.println("--------------------------\n");
    }
  }
}
```

## 状态码

在最新版本中引入了**状态码**这一概念。例如，因为IoTDB需要在写入数据之前首先注册时间序列，一种可能的解决方案是：

```
try {
    writeData();
} catch (SQLException e) {
  // the most case is that the time series does not exist
  if (e.getMessage().contains("exist")) {
      //However, using the content of the error message is not so efficient
      registerTimeSeries();
      //write data once again
      writeData();
  }
}

```

利用状态码，我们就可以不必写诸如`if (e.getErrorMessage().contains("exist"))`的代码，只需要使用`e.getStatusType().getCode() == TSStatusCode.TIME_SERIES_NOT_EXIST_ERROR.getStatusCode()`。

这里是状态码和相对应信息的列表：

|状态码|状态类型|状态信息|
|:---|:---|:---|
|200|SUCCESS_STATUS||
|201|STILL_EXECUTING_STATUS||
|202|INVALID_HANDLE_STATUS||
|300|TIMESERIES_ALREADY_EXIST_ERROR|时间序列已经存在|
|301|TIMESERIES_NOT_EXIST_ERROR|时间序列不存在|
|302|UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR|不支持的获取元数据操作|
|303|METADATA_ERROR|处理元数据错误|
|304|CHECK_FILE_LEVEL_ERROR|检查文件层级错误|
|305|OUT_OF_TTL_ERROR|插入时间少于TTL时间边界|
|306|CONFIG_ADJUSTER|IoTDB系统负载过大|
|307|MERGE_ERROR|合并错误|
|308|SYSTEM_CHECK_ERROR|系统检查错误|
|309|SYNC_DEVICE_OWNER_CONFLICT_ERROR|回传设备冲突错误|
|310|SYNC_CONNECTION_EXCEPTION|回传连接错误|
|311|STORAGE_GROUP_PROCESSOR_ERROR|存储组处理器相关错误|
|312|STORAGE_GROUP_ERROR|存储组相关错误|
|313|STORAGE_ENGINE_ERROR|存储引擎相关错误|
|400|EXECUTE_STATEMENT_ERROR|执行语句错误|
|401|SQL_PARSE_ERROR|SQL语句分析错误|
|402|GENERATE_TIME_ZONE_ERROR|生成时区错误|
|403|SET_TIME_ZONE_ERROR|设置时区错误|
|404|NOT_STORAGE_GROUP_ERROR|操作对象不是存储组|
|405|QUERY_NOT_ALLOWED|查询语句不允许|
|406|AST_FORMAT_ERROR|AST格式相关错误|
|407|LOGICAL_OPERATOR_ERROR|逻辑符相关错误|
|408|LOGICAL_OPTIMIZE_ERROR|逻辑优化相关错误|
|409|UNSUPPORTED_FILL_TYPE_ERROR|不支持的填充类型|
|410|PATH_ERROR|路径相关错误|
|405|READ_ONLY_SYSTEM_ERROR|操作系统只读|
|500|INTERNAL_SERVER_ERROR|服务器内部错误|
|501|CLOSE_OPERATION_ERROR|关闭操作错误|
|502|READ_ONLY_SYSTEM_ERROR|系统只读|
|503|DISK_SPACE_INSUFFICIENT_ERROR|磁盘空间不足|
|504|START_UP_ERROR|启动错误|
|600|WRONG_LOGIN_PASSWORD_ERROR|用户名或密码错误|
|601|NOT_LOGIN_ERROR|没有登录|
|602|NO_PERMISSION_ERROR|没有操作权限|
|603|UNINITIALIZED_AUTH_ERROR|授权人未初始化|

> 在最新版本中，我们重构了IoTDB的异常类。通过将错误信息统一提取到异常类中，并为所有异常添加不同的错误代码，从而当捕获到异常并引发更高级别的异常时，错误代码将保留并传递，以便用户了解详细的错误原因。
除此之外，我们添加了一个基础异常类“ProcessException”，由所有异常扩展。