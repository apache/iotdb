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

## JDBC

### 依赖

* JDK >= 1.8
* Maven >= 3.6



### 安装方法

在根目录下执行下面的命令:
```shell
mvn clean install -pl jdbc -am -Dmaven.test.skip=true
```



#### 在MAVEN中使用 IoTDB JDBC

```xml
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-jdbc</artifactId>
      <version>0.13.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```



#### 示例代码

本章提供了如何建立数据库连接、执行 SQL 和显示查询结果的示例。

要求您已经在工程中包含了数据库编程所需引入的包和 JDBC class.

**注意:为了更快地插入，建议使用 executeBatch()**

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
