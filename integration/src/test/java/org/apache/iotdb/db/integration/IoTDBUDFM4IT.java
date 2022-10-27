package org.apache.iotdb.db.integration;

import org.apache.iotdb.commons.udf.builtin.UDTFM4;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBUDFM4IT {

  @BeforeClass
  public static void setUp() throws Exception {
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(5)
        .setUdfTransformerMemoryBudgetInMB(5)
        .setUdfReaderMemoryBudgetInMB(5);
    EnvFactory.getEnv().initBeforeClass();
    createTimeSeries();
    generateData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(100)
        .setUdfTransformerMemoryBudgetInMB(100)
        .setUdfReaderMemoryBudgetInMB(100);
  }

  @Test
  public void test_M4_slidingTimeWindow() {
    String[] res =
        new String[] {
          "1,5.0", "10,30.0", "20,20.0", "25,8.0", "30,40.0", "45,30.0", "52,8.0", "54,18.0"
        };

    String sql =
        String.format(
            "select M4(s1, '%s'='%s','%s'='%s','%s'='%s','%s'='%s') from root.vehicle.d1",
            UDTFM4.TIME_INTERVAL_KEY,
            25,
            UDTFM4.SLIDING_STEP_KEY,
            25,
            UDTFM4.DISPLAY_WINDOW_BEGIN_KEY,
            0,
            UDTFM4.DISPLAY_WINDOW_END_KEY,
            100);

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      while (resultSet.next()) {
        String str = resultSet.getString(1) + "," + resultSet.getString(2);
        Assert.assertEquals(res[count], str);
        count++;
      }
      Assert.assertEquals(res.length, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void test_M4_slidingSizeWindow() {
    String[] res =
        new String[] {"1,5.0", "30,40.0", "33,9.0", "35,10.0", "45,30.0", "52,8.0", "54,18.0"};

    String sql =
        String.format(
            "select M4(s1,'%s'='%s','%s'='%s') from root.vehicle.d1",
            UDTFM4.WINDOW_SIZE_KEY, 10, UDTFM4.SLIDING_STEP_KEY, 10);

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      while (resultSet.next()) {
        String str = resultSet.getString(1) + "," + resultSet.getString(2);
        Assert.assertEquals(res[count], str);
        count++;
      }
      Assert.assertEquals(res.length, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void test_M4_constantTimeSeries() {
    /** Result: 0,1 24,1 25,1 49,1 50,1 74,1 75,1 99,1 */
    String sql =
        String.format(
            "select M4(s2, '%s'='%s','%s'='%s','%s'='%s','%s'='%s') from root.vehicle.d1",
            UDTFM4.TIME_INTERVAL_KEY,
            25,
            UDTFM4.SLIDING_STEP_KEY,
            25,
            UDTFM4.DISPLAY_WINDOW_BEGIN_KEY,
            0,
            UDTFM4.DISPLAY_WINDOW_END_KEY,
            100);

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      while (resultSet.next()) {
        String expStr;
        if (count % 2 == 0) {
          expStr = 25 * (count / 2) + ",1";
        } else {
          expStr = 25 * (count / 2) + 24 + ",1";
        }
        String str = resultSet.getString(1) + "," + resultSet.getString(2);
        Assert.assertEquals(expStr, str);
        count++;
      }
      Assert.assertEquals(8, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void test_EQUAL_SIZE_BUCKET_M4_SAMPLE() {
    String[] res =
        new String[] {
          "1,5.0", "8,8.0", "10,30.0", "27,20.0", "30,40.0", "45,30.0", "52,8.0", "54,18.0"
        };

    String sql = "select EQUAL_SIZE_BUCKET_M4_SAMPLE(s1,'proportion'='0.5') from root.vehicle.d1";
    // the window size is 4*(int)(1/proportion) = 8

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      while (resultSet.next()) {
        String str = resultSet.getString(1) + "," + resultSet.getString(2);
        Assert.assertEquals(res[count], str);
        count++;
      }
      Assert.assertEquals(res.length, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void test_EQUAL_SIZE_BUCKET_M4_SAMPLE_constantTimeSeries() {
    String sql = "select EQUAL_SIZE_BUCKET_M4_SAMPLE(s2, 'proportion'='0.5') from root.vehicle.d1";
    // the window size is 4*(int)(1/proportion) = 8

    try (Connection conn = EnvFactory.getEnv().getConnection();
        Statement statement = conn.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      int count = 0;
      while (resultSet.next()) {
        String expStr;
        if (count / 4 * 8 < 8 * 12) { // each 8-point window sample 4 different points
          if (count % 4 == 0) {
            expStr = 8 * (count / 4) + ",1";
          } else if (count % 4 == 1) {
            expStr = 8 * (count / 4) + 1 + ",1";
          } else if (count % 4 == 2) {
            expStr = 8 * (count / 4 + 1) - 2 + ",1";
          } else {
            expStr = 8 * (count / 4 + 1) - 1 + ",1";
          }
        } else { // the last 4 points
          expStr = count - 48 + 96 + ",1";
        }
        String str = resultSet.getString(1) + "," + resultSet.getString(2);
        Assert.assertEquals(expStr, str);
        count++;
      }
      Assert.assertEquals(52, count);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.vehicle");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s1 with datatype=double,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.vehicle.d1.s2 with datatype=INT32,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d1(timestamp,%s)" + " VALUES(%d,%d)";

  private static void generateData() {
    // data:
    // https://user-images.githubusercontent.com/33376433/151985070-73158010-8ba0-409d-a1c1-df69bad1aaee.png
    // NOTE: The last point (120,8) is commented out, because bug#7738 has not been fixed
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 20, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 25, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 54, 3));
      //      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8)); // TODO add
      // back after fixing bug#7738
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 5, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 8, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 10, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 20, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 27, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 30, 40));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 35, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 40, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 33, 9));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 45, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 52, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s1", 54, 18));
      statement.execute("FLUSH");

      for (int i = 0; i < 100; i++) {
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, "s2", i, 1));
      }
      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
