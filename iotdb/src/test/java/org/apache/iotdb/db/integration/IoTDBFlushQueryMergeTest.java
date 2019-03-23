package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.apache.iotdb.db.integration.Constant.*;
import static org.apache.iotdb.db.integration.Constant.d1s0;
import static org.junit.Assert.fail;

public class IoTDBFlushQueryMergeTest {

  private static IoTDB daemon;
  private static String[] sqls = new String[]{

      "SET STORAGE GROUP TO root.vehicle.d0", "SET STORAGE GROUP TO root.vehicle.d1",

      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",

      "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",

      "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
      "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
      "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
      "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
      "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
      "flush",
      "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
      "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
      "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",

  };

  private static IoTDBConfig iotDBConfig = IoTDBDescriptor.getInstance().getConfig();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.closeMemControl();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
    iotDBConfig.setOverflowFileSizeThreshold(0);

    insertData();

  }

  @AfterClass
  public static void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      for (String sql : sqls) {
        statement.execute(sql);
      }
      statement.close();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void selectAllSQLTest() throws ClassNotFoundException, SQLException {

    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute("select * from root");
      Assert.assertTrue(hasResultSet);

      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s0) + ","
            + resultSet.getString(d0s1) + "," + resultSet.getString(d0s2) + "," + resultSet
            .getString(d0s3)
            + "," + resultSet.getString(d1s0);
        cnt++;
      }
      statement.close();

      statement = connection.createStatement();
      statement.execute("merge");

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }
}
