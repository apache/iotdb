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

# Chapter4: Client
# Programming - JDBC

## Usage

## Dependencies

* JDK >= 1.8
* Maven >= 3.1

## Package only JDBC projects

Execute the following command in the root directory:

```
mvn clean package -pl jdbc -am -DskipTests
```

## How to install in local maven repository

In root directory:
```
mvn clean install -pl jdbc -am -Dmaven.test.skip=true
```

## Using IoTDB JDBC with Maven

```
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-jdbc</artifactId>
      <version>0.10.0</version>
    </dependency>
</dependencies>
```


## Examples

This chapter provides an example of how to open a database connection, execute a SQL query, and display the results.

Requires that you include the packages containing the JDBC classes needed for database programming.

**NOTE: For faster insertion, the insertBatch() in Session is recommended.**

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
## Status Code

**Status Code** is introduced in the latest version. For example, as IoTDB requires registering the time series first before writing data, a kind of solution is:

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

With Status Code, instead of writing codes like `if (e.getErrorMessage().contains("exist"))`, we can simply use `e.getErrorCode() == TSStatusCode.TIME_SERIES_NOT_EXIST_ERROR.getStatusCode()`.

Here is a list of Status Code and related message:

|Status Code|Status Type|Meanings|
|:---|:---|:---|
|200|SUCCESS_STATUS||
|201|STILL_EXECUTING_STATUS||
|202|INVALID_HANDLE_STATUS||
|300|TIMESERIES_ALREADY_EXIST_ERROR|Timeseries already exists|
|301|TIMESERIES_NOT_EXIST_ERROR|Timeseries does not exist|
|302|UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR|Unsupported fetch metadata operation|
|303|METADATA_ERROR|Meet error when dealing with metadata|
|305|OUT_OF_TTL_ERROR|Insertion time is less than TTL time bound|
|306|CONFIG_ADJUSTER|IoTDB system load is too large|
|307|MERGE_ERROR|Meet error while merging|
|308|SYSTEM_CHECK_ERROR|Meet error while system checking|
|309|SYNC_DEVICE_OWNER_CONFLICT_ERROR|Sync device owners conflict|
|310|SYNC_CONNECTION_EXCEPTION|Meet error while sync connecting|
|311|STORAGE_GROUP_PROCESSOR_ERROR|Storage group processor related error|
|312|STORAGE_GROUP_ERROR|Storage group related error|
|313|STORAGE_ENGINE_ERROR|Storage engine related error|
|400|EXECUTE_STATEMENT_ERROR|Execute statement error|
|401|SQL_PARSE_ERROR|Meet error while parsing SQL|
|402|GENERATE_TIME_ZONE_ERROR|Meet error while generating time zone|
|403|SET_TIME_ZONE_ERROR|Meet error while setting time zone|
|404|NOT_STORAGE_GROUP_ERROR|Operating object is not a storage group|
|405|QUERY_NOT_ALLOWED|Query statements are not allowed error|
|406|AST_FORMAT_ERROR|AST format related error|
|407|LOGICAL_OPERATOR_ERROR|Logical operator related error|
|408|LOGICAL_OPTIMIZE_ERROR|Logical optimize related error|
|409|UNSUPPORTED_FILL_TYPE_ERROR|Unsupported fill type related error|
|410|PATH_ERROR|Path related error|
|500|INTERNAL_SERVER_ERROR|Internal server error|
|501|CLOSE_OPERATION_ERROR|Meet error in close operation|
|502|READ_ONLY_SYSTEM_ERROR|Operating system is read only|
|503|DISK_SPACE_INSUFFICIENT_ERROR|Disk space is insufficient|
|504|START_UP_ERROR|Meet error while starting up|
|600|WRONG_LOGIN_PASSWORD_ERROR|Username or password is wrong|
|601|NOT_LOGIN_ERROR|Has not logged in|
|602|NO_PERMISSION_ERROR|No permissions for this operation|
|603|UNINITIALIZED_AUTH_ERROR|Uninitialized authorizer|

> All exceptions are refactored in latest version by extracting uniform message into exception classes. Different error codes are added to all exceptions. When an exception is caught and a higher-level exception is thrown, the error code will keep and pass so that users will know the detailed error reason.
A base exception class "ProcessException" is also added to be extended by all exceptions.
