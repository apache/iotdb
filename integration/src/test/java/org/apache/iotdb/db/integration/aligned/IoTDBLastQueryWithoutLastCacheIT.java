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
package org.apache.iotdb.db.integration.aligned;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.db.constant.TestConstant.DATA_TYPE_STR;
import static org.apache.iotdb.db.constant.TestConstant.TIMESEIRES_STR;
import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.db.constant.TestConstant.VALUE_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class})
public class IoTDBLastQueryWithoutLastCacheIT {

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;
  protected static boolean enableLastCache;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    // TODO When the aligned time series support compaction, we need to set compaction to true
    enableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    enableLastCache = IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableLastCache(false);

    AlignedWriteUtil.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    IoTDBDescriptor.getInstance().getConfig().setEnableLastCache(enableLastCache);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void selectAllAlignedLastTest() throws ClassNotFoundException {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "23,root.sg1.d1.s1,230000.0,FLOAT",
                "40,root.sg1.d1.s2,40,INT32",
                "30,root.sg1.d1.s3,30,INT64",
                "30,root.sg1.d1.s4,false,BOOLEAN",
                "40,root.sg1.d1.s5,aligned_test40,TEXT"));

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select last * from root.sg1.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllAlignedAndNonAlignedLastTest() throws ClassNotFoundException {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "23,root.sg1.d1.s1,230000.0,FLOAT",
                "40,root.sg1.d1.s2,40,INT32",
                "30,root.sg1.d1.s3,30,INT64",
                "30,root.sg1.d1.s4,false,BOOLEAN",
                "40,root.sg1.d1.s5,aligned_test40,TEXT",
                "20,root.sg1.d2.s1,20.0,FLOAT",
                "40,root.sg1.d2.s2,40,INT32",
                "30,root.sg1.d2.s3,30,INT64",
                "30,root.sg1.d2.s4,false,BOOLEAN",
                "40,root.sg1.d2.s5,non_aligned_test40,TEXT"));

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select last * from root.sg1.*");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllAlignedLastWithTimeFilterTest() throws ClassNotFoundException {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList("40,root.sg1.d1.s2,40,INT32", "40,root.sg1.d1.s5,aligned_test40,TEXT"));

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select last * from root.sg1.d1 where time > 30");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedLastTest1() throws ClassNotFoundException {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "23,root.sg1.d1.s1,230000.0,FLOAT",
                "30,root.sg1.d1.s4,false,BOOLEAN",
                "40,root.sg1.d1.s5,aligned_test40,TEXT"));

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select last s1, s4, s5 from root.sg1.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedLastTest2() throws ClassNotFoundException {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList("23,root.sg1.d1.s1,230000.0,FLOAT", "30,root.sg1.d1.s4,false,BOOLEAN"));

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select last s1, s4 from root.sg1.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedLastWithTimeFilterTest() throws ClassNotFoundException {

    Set<String> retSet =
        new HashSet<>(Collections.singletonList("40,root.sg1.d1.s5,aligned_test40,TEXT"));

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select last s1, s4, s5 from root.sg1.d1 where time > 30");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedAndNonAlignedLastWithTimeFilterTest() throws ClassNotFoundException {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "40,root.sg1.d1.s5,aligned_test40,TEXT",
                "40,root.sg1.d2.s5,non_aligned_test40,TEXT"));

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // 1 4 5
      boolean hasResultSet =
          statement.execute(
              "select last d2.s5, d1.s4, d2.s1, d1.s5, d2.s4, d1.s1 from root.sg1 where time > 30");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertTrue(retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
