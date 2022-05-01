package org.apache.iotdb.db.integration;

import org.apache.iotdb.integration.env.EnvFactory;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class IoTDBKeywordQueryIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testQuery() throws SQLException {
    try (Connection conn = EnvFactory.getEnv().getConnection()) {
      String[] sensorNames = new String[] {"select", "www.baidu.com"};
      try (Statement stmt = conn.createStatement()) {
        for (String sensor : sensorNames) {
          stmt.execute("CREATE TIMESERIES root.sg.d1." + columnWrapper(sensor) + " int32");
        }
      }
      try (PreparedStatement stmt =
          conn.prepareStatement("INSERT INTO root.sg.d1(time, ?) values (?, ?)")) {
        for (String sensor : sensorNames) {
          stmt.setString(1, sensor);
          stmt.setLong(2, 1L);
          stmt.setInt(3, 1);
        }
      }
      try (Statement stmt = conn.createStatement()) {
        for (String sensor : sensorNames) {
          try (ResultSet rs =
              stmt.executeQuery("SELECT " + columnWrapper(sensor) + " FROM root.sg.d1")) {
            Assert.assertTrue(rs.next());
            long timestamp = rs.getLong("Time");
            Assert.assertEquals(1L, timestamp);
            int value = rs.getInt("root.sg.d1." + columnWrapper(sensor));
            Assert.assertEquals(1, value);
            Assert.assertFalse(rs.next());
          }
        }
      }
    }
  }

  /**
   * columnWrapper is a special helper function to make business domain words compatible with IoTDB
   * keywords
   */
  private String columnWrapper(String column) {
    return String.format("`%s`", column);
  }
}
