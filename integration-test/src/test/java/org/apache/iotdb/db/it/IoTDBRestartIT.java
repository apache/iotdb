/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.it;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.utils.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

// @Ignore
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBRestartIT {

  private final Logger logger = LoggerFactory.getLogger(IoTDBRestartIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setWalMode("SYNC");
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus");
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    EnvFactory.getEnv().getConfig().getCommonConfig().setWalMode("ASYNC");
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass("org.apache.iotdb.consensus.simple.SimpleConsensus");
  }

  @Test
  public void testRestart() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1.0)");
      statement.execute("flush");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,1.0)");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,1.0)");

      String[] exp = new String[] {"1,1.0", "2,1.0", "3,1.0"};
      int cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.turbine.d1")) {
        assertNotNull(resultSet);
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
    }
  }

  @Test
  public void testRestartDelete() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,2)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,3)");
    }

    TestUtils.restartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("delete from root.turbine.d1.s1 where time<=1");

      ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.turbine.d1");
      assertNotNull(resultSet);
      String[] exp = new String[] {"2,2.0", "3,3.0"};
      int cnt = 0;
      try {
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }

        statement.execute("flush");
        statement.execute("delete from root.turbine.d1.s1 where time<=2");

        exp = new String[] {"3,3.0"};
        resultSet = statement.executeQuery("SELECT s1 FROM root.turbine.d1");
        assertNotNull(resultSet);
        cnt = 0;
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      } finally {
        resultSet.close();
      }
    }
  }

  @Test
  public void testRestartQueryLargerThanEndTime() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,2)");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(4,2)");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] exp =
          new String[] {
            "4,2.0",
          };
      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1 FROM root.turbine.d1 where time > 3")) {
        assertNotNull(resultSet);
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
      assertEquals(1, cnt);
    }
  }

  @Test
  public void testRestartEndTime() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,2)");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s2) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s2) values(2,2)");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] exp = new String[] {"1,1.0", "2,2.0"};
      int cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("SELECT s2 FROM root.turbine.d1")) {
        assertNotNull(resultSet);
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
      assertEquals(2, cnt);
    }
  }

  @Test
  public void testRecoverWALMismatchDataType() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine1.d1(timestamp,s1,s2) values(1,1.1,2.2)");
      statement.execute("delete timeseries root.turbine1.d1.s1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT32, encoding=RLE, compression=SNAPPY");
    }

    TestUtils.restartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select * from root.**")) {
        assertNotNull(resultSet);
        int cnt = 0;
        assertEquals(3, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          assertEquals("1", resultSet.getString(1));
          assertNull(resultSet.getString(2));
          assertEquals("2.2", resultSet.getString(3));
          cnt++;
        }
        assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void testRecoverWALDeleteSchema() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine1.d1(timestamp,s1,s2) values(1,1.1,2.2)");
      statement.execute("delete timeseries root.turbine1.d1.s1");
    }

    TestUtils.restartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select * from root.**")) {
        assertNotNull(resultSet);
        int cnt = 0;
        assertEquals(2, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          assertEquals("1", resultSet.getString(1));
          assertEquals("2.2", resultSet.getString(2));
          cnt++;
        }
        assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void testRecoverWALDeleteSchemaCheckResourceTime() throws Exception {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    int avgSeriesPointNumberThreshold = config.getAvgSeriesPointNumberThreshold();
    config.setAvgSeriesPointNumberThreshold(2);
    long tsFileSize = config.getSeqTsFileSize();
    long unFsFileSize = config.getSeqTsFileSize();
    config.setSeqTsFileSize(10000000);
    config.setUnSeqTsFileSize(10000000);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.turbine1.d1.s1 with datatype=INT64");
      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(2,1)");
      statement.execute("create timeseries root.turbine1.d1.s2 with datatype=BOOLEAN");
      statement.execute("insert into root.turbine1.d1(timestamp,s2) values(3,true)");
      statement.execute("insert into root.turbine1.d1(timestamp,s2) values(4,true)");
    }

    TestUtils.restartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      long[] result = new long[] {1L, 2L};
      ResultSet resultSet =
          statement.executeQuery("select s1 from root.turbine1.d1 where time < 3");
      assertNotNull(resultSet);
      int cnt = 0;
      while (resultSet.next()) {
        assertEquals(resultSet.getLong(1), result[cnt]);
        cnt++;
      }
      assertEquals(2, cnt);
    }

    config.setAvgSeriesPointNumberThreshold(avgSeriesPointNumberThreshold);
    config.setSeqTsFileSize(tsFileSize);
    config.setUnSeqTsFileSize(unFsFileSize);
  }

  @Test
  public void testRecoverFromFlushMemTableError() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine1.d1(timestamp,s1,s2) values(1,1.1,2.2)");
    }

    // mock exception during flush memtable
    TestUtils.restartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select * from root.**")) {
        assertNotNull(resultSet);
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals("1", resultSet.getString(1));
          assertEquals("1.1", resultSet.getString(2));
          assertEquals("2.2", resultSet.getString(3));
          cnt++;
        }
        assertEquals(1, cnt);
      }
    }
  }
}
