package org.apache.iotdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCExample {

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try (Connection connection = DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      long startTime = System.currentTimeMillis();
      ResultSet resultSet = statement.executeQuery("select * from root where time < 10000000");
      outputResult(resultSet);
      long endTime = System.currentTimeMillis();
      System.out.println("Cost: " + (endTime - startTime));
    }
  }

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
      }
      System.out.println("--------------------------\n");
    }
  }
}
