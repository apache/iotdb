package org.apache.iotdb.jdbc;

import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class WriteDemo {

  public static void main(String args[]) throws ClassNotFoundException {
    int cols = 2, rows = 10;
    Config.rpcThriftCompressionEnable = true;
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try {
      Connection conn = DriverManager.getConnection("jdbc:iotdb://192.168.130.19:6667/", "root", "root");
      IoTDBStatement statement = (IoTDBStatement) conn.createStatement();
      statement.execute("SET STORAGE GROUP TO root.ln.wf01.cols" + cols);
      for (int i = 0; i < cols; i++)
        statement.execute("CREATE TIMESERIES root.ln.wf01.cols" + cols + ".status" + i + " WITH DATATYPE=DOUBLE, ENCODING=PLAIN");
      for (int i = 0; i < rows; i++) {
        String statementStr = "";
        for (int j = 0; j < cols; j++) statementStr += ",status" + j;
        String valueStr = "";
        for (int j = 0; j < cols; j++) valueStr += ",123456.123456";
        statementStr = "insert into root.ln.wf01.cols" + cols + "(timestamp" + statementStr + ") values(" + (i + 1) + valueStr + ")";
        statement.execute(statementStr);
      }
      conn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }

  }
}
