/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBMultiSeriesIT {

  private static boolean testFlag = TestConstant.testFlag;
  private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  private static int maxNumberOfPointsInPage;
  private static int pageSizeInByte;
  private static int groupSizeInByte;
  private static long prevPartitionInterval;

  @BeforeClass
  public static void setUp() throws Exception {

    EnvironmentUtils.closeStatMonitor();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionStrategy(CompactionStrategy.NO_COMPACTION);

    // use small page setting
    // origin value
    maxNumberOfPointsInPage = tsFileConfig.getMaxNumberOfPointsInPage();
    pageSizeInByte = tsFileConfig.getPageSizeInByte();
    groupSizeInByte = tsFileConfig.getGroupSizeInByte();

    // new value
    tsFileConfig.setMaxNumberOfPointsInPage(1000);
    tsFileConfig.setPageSizeInByte(1024 * 150);
    tsFileConfig.setGroupSizeInByte(1024 * 1000);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(1024 * 1000);
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(100);
    TSFileDescriptor.getInstance().getConfig().setCompressor("LZ4");

    EnvironmentUtils.envSetUp();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // recovery value
    tsFileConfig.setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);
    tsFileConfig.setPageSizeInByte(pageSizeInByte);
    tsFileConfig.setGroupSizeInByte(groupSizeInByte);
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(prevPartitionInterval);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(groupSizeInByte);
    TSFileDescriptor.getInstance().getConfig().setCompressor("SNAPPY");
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionStrategy(CompactionStrategy.LEVEL_COMPACTION);
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : TestConstant.create_sql) {
        statement.execute(sql);
      }

      statement.execute("SET STORAGE GROUP TO root.fans");
      statement.execute("CREATE TIMESERIES root.fans.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.fans.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE");

      // insert of data time range : 1-1000 into fans
      for (int time = 1; time < 1000; time++) {

        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.execute(sql);
        sql =
            String.format("insert into root.fans.d0(timestamp,s1) values(%s,%s)", time, time % 40);
        statement.execute(sql);
      }

      // insert large amount of data time range : 13700 ~ 24000
      for (int time = 13700; time < 24000; time++) {

        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
        statement.execute(sql);
      }

      // insert large amount of data time range : 3000 ~ 13600
      for (int time = 3000; time < 13600; time++) {
        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')",
                time, TestConstant.stringValue[time % 5]);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s4) values(%s, %s)",
                time, TestConstant.booleanValue[time % 2]);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, time);
        statement.execute(sql);
      }

      statement.execute("flush");
      statement.execute("merge");

      // buffwrite data, unsealed file
      for (int time = 100000; time < 101000; time++) {

        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
        statement.execute(sql);
      }

      statement.execute("flush");

      // sequential data, memory data
      for (int time = 200000; time < 201000; time++) {

        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, -time % 20);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, -time % 30);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, -time % 77);
        statement.execute(sql);
      }

      // unseq insert, time < 3000
      for (int time = 2000; time < 2500; time++) {

        String sql =
            String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')",
                time, TestConstant.stringValue[time % 5]);
        statement.execute(sql);
      }

      // seq insert, time > 200000
      for (int time = 200900; time < 201000; time++) {

        String sql =
            String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 6666);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, 7777);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, 8888);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, "goodman");
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s4) values(%s, %s)",
                time, TestConstant.booleanValue[time % 2]);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, 9999);
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // "select * from root.vehicle" : test select wild data
  @Test
  public void selectAllTest() throws ClassNotFoundException {
    String selectSql = "select * from root";

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(selectSql);
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.fans.d0.s0")
                  + ","
                  + resultSet.getString("root.fans.d0.s1");
          cnt++;
        }
        assertEquals(24399, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // "select * from root.vehicle" : test select wild data
  @Test
  public void selectAllFromVehicleTest() throws ClassNotFoundException {
    String selectSql = "select * from root.vehicle";

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(selectSql);
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(23400, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // "select s0 from root.vehicle.d0 where s0 >= 20" : test select same series with same series
  // filter
  @Test
  public void selectOneSeriesWithValueFilterTest() throws ClassNotFoundException {

    String selectSql = "select s0 from root.vehicle.d0 where s0 >= 20";

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute(selectSql);
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s0);
          cnt++;
        }
        assertEquals(16440, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // "select s0 from root.vehicle.d0 where time > 22987 " : test select clause with only global time
  // filter
  @Test
  public void seriesGlobalTimeFilterTest() throws ClassNotFoundException {

    Class.forName(Config.JDBC_DRIVER_NAME);

    boolean hasResultSet;

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      hasResultSet = statement.execute("select s0 from root.vehicle.d0 where time > 22987");
      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TestConstant.TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(
                      TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s0);
          cnt++;
        }

        assertEquals(3012, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // "select s1 from root.vehicle.d0 where s0 < 111" : test select clause with different series
  // filter
  @Test
  public void crossSeriesReadUpdateTest() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    boolean hasResultSet;

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      hasResultSet = statement.execute("select s1 from root.vehicle.d0 where s0 < 111");
      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          long time = Long.parseLong(resultSet.getString(TestConstant.TIMESTAMP_STR));
          String value =
              resultSet.getString(TestConstant.d0 + IoTDBConstant.PATH_SEPARATOR + TestConstant.s1);
          if (time > 200900) {
            assertEquals("7777", value);
          }
          // String ans = resultSet.getString(d0s1);
          cnt++;
        }
        assertEquals(22800, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectUnknownTimeSeries() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s0, s10 from root.vehicle.*");
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(23400, cnt);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void selectWhereUnknownTimeSeries() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s0 from root.vehicle.d0 where s10 < 111");
      fail("not throw exception when unknown time series in where clause");
    } catch (SQLException e) {
      assertTrue(
          e.getMessage().contains("Unknown time series root.vehicle.d0.s10 in `where clause`"));
    }
  }

  @Test
  public void selectWhereUnknownTimeSeriesFromRoot() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "select s1 from root.vehicle.d0 where root.vehicle.d0.s0 < 111 and root.vehicle.d0.s10 < 111");
      fail("not throw exception when unknown time series in where clause");
    } catch (SQLException e) {
      assertEquals(
          "411: Error occurred in query process: Path [root.vehicle.d0.s10] does not exist",
          e.getMessage());
    }
  }

  @Test
  public void testCreateTimeSeriesWithoutEncoding() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.name WITH DATATYPE=TEXT");
      statement.execute(
          "CREATE TIMESERIES root.ln.wf01.wt01.age WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.salary WITH DATATYPE=INT64");
      statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.score WITH DATATYPE=FLOAT");
      statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.grade WITH DATATYPE=DOUBLE");

      ResultSet nameRs = statement.executeQuery("SHOW TIMESERIES root.ln.wf01.wt01.name");
      nameRs.next();
      Assert.assertTrue(TSEncoding.PLAIN.name().equalsIgnoreCase(nameRs.getString(5)));

      ResultSet ageRs = statement.executeQuery("SHOW TIMESERIES root.ln.wf01.wt01.age");
      ageRs.next();
      Assert.assertTrue(TSEncoding.RLE.name().equalsIgnoreCase(ageRs.getString(5)));

      ResultSet salaryRs = statement.executeQuery("SHOW TIMESERIES root.ln.wf01.wt01.salary");
      salaryRs.next();
      Assert.assertTrue(TSEncoding.RLE.name().equalsIgnoreCase(salaryRs.getString(5)));

      ResultSet scoreRs = statement.executeQuery("SHOW TIMESERIES root.ln.wf01.wt01.score");
      scoreRs.next();
      Assert.assertTrue(TSEncoding.GORILLA.name().equalsIgnoreCase(scoreRs.getString(5)));

      ResultSet gradeRs = statement.executeQuery("SHOW TIMESERIES root.ln.wf01.wt01.grade");
      gradeRs.next();
      Assert.assertTrue(TSEncoding.GORILLA.name().equalsIgnoreCase(gradeRs.getString(5)));
    }
  }
}
