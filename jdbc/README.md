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

# Usage

## Dependencies

* JDK >= 1.8
* Maven >= 3.0

## How to package only jdbc project

In root directory:
> mvn clean package -pl jdbc -am -Dmaven.test.skip=true

## How to install in local maven repository

In root directory:
> mvn clean install -pl jdbc -am -Dmaven.test.skip=true

## Using IoTDB JDBC with Maven

```
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-jdbc</artifactId>
      <version>0.8.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```


## Examples

This chapter provides an example of how to open a database connection, execute a SQL query, and display the results.

Requires that you include the packages containing the JDBC classes needed for database programming.

```Java
import java.sql.*;

  /**
   * Before executing an SQL statement with a Statement object, you need to create a Statement object using the createStatement() method of the Connection object.
   * After creating a Statement object, you can use its execute() method to execute an SQL statement
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
    statement.execute("SET STORAGE GROUP TO root.demo");

    //Show storage group
    statement.execute("SHOW STORAGE GROUP");
    outputResult(statement.getResultSet());

    //Create time series
    //Different data type has different encoding methods. Here use INT32 as an example.
    statement.execute("CREATE TIMESERIES root.demo.s0 WITH DATATYPE=INT32,ENCODING=RLE;");

    //Show time series
    statement.execute("SHOW TIMESERIES root.demo");
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
    statement.execute("select * from root.demo");
    outputResult(statement.getResultSet());

    //Exact query statement
    statement.execute("select s0 from root.demo where time = 4;");
    outputResult(statement.getResultSet());

    //Time range query
    statement.execute("select s0 from root.demo where time >= 2 and time < 5;");
    outputResult(statement.getResultSet());

    //Aggregate query
    statement.execute("select count(s0) from root.demo;");
    outputResult(statement.getResultSet());

    //Latest time point query
    statement.execute("select max_time(s0) from root.demo;");
    outputResult(statement.getResultSet());

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
```
