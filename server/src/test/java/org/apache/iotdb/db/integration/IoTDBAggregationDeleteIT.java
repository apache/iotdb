package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.junit.Assert.*;

public class IoTDBAggregationDeleteIT {


  private static String[] dataSet = new String[]{
          "INSERT INTO root.turbine.d1(timestamp,s1) values(1,1)",
          "INSERT INTO root.turbine.d1(timestamp,s1) values(2,2)",
          "INSERT INTO root.turbine.d1(timestamp,s1) values(3,3)",
          "INSERT INTO root.turbine.d1(timestamp,s1) values(4,4)",
          "INSERT INTO root.turbine.d1(timestamp,s1) values(5,5)",
          "flush",
          "delete from root.turbine.d1.s1 where time < 3"
  };

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(1000);
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(86400);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void countAfterDeleteTest() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select count(*) from root");

      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          assertEquals("3", resultSet.getString(count("root.turbine.d1.s1")));
        }
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  private void prepareData() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                    "root");
         Statement statement = connection.createStatement()) {

      for (String sql : dataSet) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
