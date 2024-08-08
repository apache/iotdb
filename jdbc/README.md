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

## Usage

## Dependencies

* JDK >= 1.8
* Maven >= 3.6

## How to package only jdbc project

In root directory:
```
mvn clean package -pl jdbc -am -Dmaven.test.skip=true
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
      <version>1.0.0</version>
    </dependency>
</dependencies>
```


## Examples

This chapter provides an example of how to open a database connection, execute a SQL query, and display the results.

Requires that you include the packages containing the JDBC classes needed for database programming.

**NOTE: For faster insertion, the insertTablet() in Session is recommended.**

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
    //Create database
    try {
      statement.execute("CREATE DATABASE root.demo");
    }catch (IoTDBSQLException e){
      System.out.println(e.getMessage());
    }


    //SHOW DATABASES
    statement.execute("SHOW DATABASES");
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

|Status Code|Status Type|Meaning|
|:---|:---|:---|
|200|SUCCESS_STATUS||
|201|STILL_EXECUTING_STATUS||
|202|INVALID_HANDLE_STATUS||
|301|TIMESERIES_NOT_EXIST_ERROR|Timeseries does not exist|
|302|UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR|Unsupported fetch metadata operation|
|303|FETCH_METADATA_ERROR|Failed to fetch metadata|
|400|EXECUTE_STATEMENT_ERROR|Execute statement error|
|401|SQL_PARSE_ERROR|Meet error while parsing SQL|
|402|GENERATE_TIME_ZONE_ERROR|Meet error while generating time zone|
|403|SET_TIME_ZONE_ERROR|Meet error while setting time zone|
|404|NOT_A_STORAGE_GROUP_ERROR|Operating object is not a database|
|405|READ_ONLY_SYSTEM_ERROR|Operating system is read only|
|500|INTERNAL_SERVER_ERROR|Internal server error|
|600|WRONG_LOGIN_PASSWORD_ERROR|Username or password is wrong|
|601|NOT_LOGIN_ERROR|Has not logged in|
|602|NO_PERMISSION_ERROR|No permissions for this operation, please add privilege|
|603|UNINITIALIZED_AUTH_ERROR|Uninitialized authorizer|

##How to try IoTDB JDBC using Karaf
Suppose you have started Karaf 4.2.8.

Put the iotdb-jdbc jars into your maven repository:
`mvn install -DskipTests -pl iotdb-jdbc -am`

In your Karaf client shell:
feature:repo-add mvn:org.apache.iotdb/iotdb-jdbc/1.0.0/xml/features

NOTE: that you must install JDBC first and then iotdb-feature

```ini
feature:install jdbc 
feature:install iotdb-feature
```
Start a data source instance (supposed you have started IoTDB):

```
jdbc:ds-create -dc org.apache.iotdb.jdbc.IoTDBDriver -u root -p root -url "jdbc:iotdb://127.0.0.1:6667/" IoTDB
```
Try query:
`jdbc:query IoTDB "select * from root"`