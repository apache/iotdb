package org.apache.iotdb.db.it.udf;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.constant.UDFTestConstant;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBUDFWindowQuery2IT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setUdfMemoryBudgetInMB(5);
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg");
      statement.execute("CREATE TIMESERIES root.sg.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s2 with datatype=INT32,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    // SessionWindow
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg.d1(time, s1, s2) values (1, 1, 1)");
      statement.execute("insert into root.sg.d1(time, s1, s2) values (2, 2, 2)");
      statement.execute("insert into root.sg.d1(time, s1, s2) values (3, 3, 3)");
      statement.execute("insert into root.sg.d1(time, s1, s2) values (9, 9, 9)");
      statement.execute("insert into root.sg.d1(time, s1, s2) values (5, 5, 5)");
      statement.execute("insert into root.sg.d1(time, s1, s2) values (12, 12, 12)");
      statement.execute("insert into root.sg.d1(time, s1, s2) values (14, 14, 14)");
      statement.execute("insert into root.sg.d1(time, s1, s2) values (18, 18, 18)");
      statement.execute("insert into root.sg.d1(time, s1, s2) values (21, 21, 21)");
      statement.execute("insert into root.sg.d1(time, s1, s2) values (24, 24, 24)");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function window_start_end as 'org.apache.iotdb.db.query.udf.example.WindowStartEnd'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testSessionTimeWindow1() {
    long[] windowStart = new long[] {12, 18};
    long[] windowEnd = new long[] {14, 21};
    testSessionTimeWindowSS("3", windowStart, windowEnd, 12L, 24L);
  }

  @Test
  public void testSessionTimeWindow2() {
    long[] windowStart = new long[] {12, 18};
    long[] windowEnd = new long[] {14, 24};
    testSessionTimeWindowSS("3", windowStart, windowEnd, 12L, Long.MAX_VALUE);
  }

  @Test
  public void testStateTimeWindow() {
    long[] windowStart = new long[] {12, 18};
    long[] windowEnd = new long[] {14, 21};
    testStateWindowSS("3", windowStart, windowEnd, 12L, 24L);
  }

  private void testSessionTimeWindowSS(
      String sessionGap, long[] windowStart, long[] windowEnd, Long displayBegin, Long displayEnd) {
    String sql;
    if (displayBegin == null) {
      sql =
          String.format(
              "select window_start_end(s1, '%s'='%s', '%s'='%s') from root.sg.d1",
              UDFTestConstant.ACCESS_STRATEGY_KEY,
              UDFTestConstant.ACCESS_STRATEGY_SESSION,
              UDFTestConstant.SESSION_GAP_KEY,
              sessionGap);
    } else {
      sql =
          String.format(
              "select window_start_end(s1, '%s'='%s', '%s'='%s', '%s'='%s', '%s'='%s') from root.sg.d1",
              UDFTestConstant.ACCESS_STRATEGY_KEY,
              UDFTestConstant.ACCESS_STRATEGY_SESSION,
              UDFTestConstant.DISPLAY_WINDOW_BEGIN_KEY,
              displayBegin,
              UDFTestConstant.DISPLAY_WINDOW_END_KEY,
              displayEnd,
              UDFTestConstant.SESSION_GAP_KEY,
              sessionGap);
    }

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertEquals(2, resultSet.getMetaData().getColumnCount());
      int cnt = 0;
      while (resultSet.next()) {
        Assert.assertEquals(resultSet.getLong(1), windowStart[cnt]);
        Assert.assertEquals(resultSet.getLong(2), windowEnd[cnt]);
        cnt++;
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void testStateWindowSS(
      String delta, long[] windowStart, long[] windowEnd, Long displayBegin, Long displayEnd) {
    String sql;
    if (displayBegin == null) {
      if (delta == null) {
        sql =
            String.format(
                "select window_start_end(%s, '%s'='%s') from root.sg.d1",
                "s2", UDFTestConstant.ACCESS_STRATEGY_KEY, UDFTestConstant.ACCESS_STRATEGY_STATE);
      } else {
        sql =
            String.format(
                "select window_start_end(%s, '%s'='%s', '%s'='%s') from root.sg.d1",
                "s2",
                UDFTestConstant.ACCESS_STRATEGY_KEY,
                UDFTestConstant.ACCESS_STRATEGY_STATE,
                UDFTestConstant.STATE_DELTA_KEY,
                delta);
      }
    } else {
      sql =
          String.format(
              "select window_start_end(%s, '%s'='%s', '%s'='%s', '%s'='%s', '%s'='%s') from root.sg.d1",
              "s2",
              UDFTestConstant.ACCESS_STRATEGY_KEY,
              UDFTestConstant.ACCESS_STRATEGY_STATE,
              UDFTestConstant.DISPLAY_WINDOW_BEGIN_KEY,
              displayBegin,
              UDFTestConstant.DISPLAY_WINDOW_END_KEY,
              displayEnd,
              UDFTestConstant.STATE_DELTA_KEY,
              delta);
    }

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertEquals(2, resultSet.getMetaData().getColumnCount());
      int cnt = 0;
      while (resultSet.next()) {
        Assert.assertEquals(resultSet.getLong(1), windowStart[cnt]);
        Assert.assertEquals(resultSet.getLong(2), windowEnd[cnt]);
        cnt++;
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
