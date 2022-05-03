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
          stmt.setString(1, columnWrapper(sensor));
          stmt.setLong(2, 1L);
          stmt.setInt(3, 1);
          stmt.execute();
        }
      }
      try (Statement stmt = conn.createStatement()) {
        for (String sensor : sensorNames) {
          try (ResultSet rs =
              stmt.executeQuery("SELECT " + columnWrapper(sensor) + " FROM root.sg.d1")) {
            System.out.println("Query sensor: " + sensor);
            Assert.assertTrue(sensor, rs.next());
            long timestamp = rs.getLong("Time");
            Assert.assertEquals(1L, timestamp);
            // Here is the confusion
            // Query root.sg.d1.select OK, but root.sg.d1.www.baidu.com is wrong
            int value = rs.getInt("root.sg.d1." + sensor);
            // Query root.sg.d1.`www.baidu.com` is OK, but root.sg.d1.`select` is wrong.
            // int value = rs.getInt("root.sg.d1." + columnWrapper(sensor));
            // So the user can't know when to use `` to wrap their business domain words.
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
    if (column.contains("`")) {
      column = column.replace("`", "``");
    }
    return String.format("`%s`", column);
  }
}
