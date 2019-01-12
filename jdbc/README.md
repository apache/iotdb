# Usage

## Dependencies

* JDK >= 1.8
* Maven >= 3.0

## How to package only jdbc project

In root directory:
> mvn clean package -pl iotdb-jdbc -am -Dmaven.test.skip=true

## How to install in local maven repository

In root directory:
> mvn clean install -pl iotdb-jdbc -am -Dmaven.test.skip=true

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

## Example
(for more detailes, please see example/src/main/java/org/apache/iotdb/jdbc/jdbcDemo/SendDataToIotdb.java)
```Java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Example {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.iotdb.jdbc.TsfileDriver");
        Connection connection = null;
        Statement statement = null;
        try {
            connection =  DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            statement = connection.createStatement();
            statement.execute("select s1 from root.laptop.d1");
            ResultSet resultSet = statement.getResultSet();
            while(resultSet.next()){
                System.out.println(String.format("timestamp %s, value %s", resultSet.getString(1), resultSet.getString(2)));
            }
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
}
```
