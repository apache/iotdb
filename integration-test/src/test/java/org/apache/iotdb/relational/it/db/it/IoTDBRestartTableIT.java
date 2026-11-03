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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.it.utils.DeletionTestUtils;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.utils.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBRestartTableIT {

  private final Logger logger = LoggerFactory.getLogger(IoTDBRestartTableIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setWalMode("SYNC")
        .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.simple.SimpleConsensus")
        .setDataReplicationFactor(1)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setMemtableSizeThreshold(16 * 1024 * 1024)
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
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
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database test");
      statement.execute("use \"test\"");
      statement.execute("create table turbine (tag1 string tag, s1 float field)");
      statement.execute("insert into turbine(tag1, time,s1) values('d1', 1,1.0)");
      statement.execute("flush");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute("insert into turbine(tag1, time,s1) values('d1', 2,1.0)");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute("insert into turbine(tag1, time,s1) values('d1', 3,1.0)");

      String[] exp = new String[] {"1,1.0", "2,1.0", "3,1.0"};
      int cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("SELECT time, s1 FROM turbine")) {
        assertNotNull(resultSet);
        while (resultSet.next()) {
          String result = resultSet.getLong("time") + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
    }
  }

  @Test
  public void testRestartDelete() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      connection.setClientInfo("time_zone", "+00:00");
      statement.execute("create database test");
      statement.execute("use \"test\"");
      statement.execute(
          "create table restart_delete(device_id string tag, site string tag, "
              + "s1 int64 field, s2 boolean field)");
      statement.execute(
          "create table restart_keep(device_id string tag, site string tag, "
              + "s1 int64 field, s2 boolean field)");
      statement.execute(
          "insert into restart_delete(time,device_id,site,s1,s2) "
              + "values(1,'d1','north',11,true),(2,'d1','north',12,false),"
              + "(2,'d1','south',212,true),(2,'d1',null,52,true),(2,'d1','',62,false)");
      statement.execute(
          "insert into restart_keep(time,device_id,site,s1,s2) "
              + "values(1,'d1','north',311,true),(2,'d1','north',312,false)");
      statement.execute("flush");
      // Identical TAG text in two tables, and NULL/empty trailing TAGs, coexist in one file.
      DeletionTestUtils.assertSharedTsFile(
          "test",
          IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"restart_delete", "d1", "north"}),
          IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"restart_delete", "d1", "south"}),
          IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"restart_delete", "d1", null}),
          IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"restart_delete", "d1", ""}),
          IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"restart_keep", "d1", "north"}));
      List<Path> closedFiles = DeletionTestUtils.getClosedTsFiles("test", true);
      statement.execute(
          "insert into restart_delete(time,device_id,site,s1) values(1,'d1','north',111)");
      statement.execute(
          "insert into restart_delete(time,device_id,site,s1,s2) "
              + "values(3,'d1','north',13,true),(4,'d1','north',14,false)");
      assertEquals(closedFiles, DeletionTestUtils.getClosedTsFiles("test", true));
      assertTrue(DeletionTestUtils.getClosedTsFiles("test", false).isEmpty());

      statement.execute(
          "delete from restart_delete where device_id='d1' and site='north' and time <= 3");
      assertRestartTableData(statement, "1970-01-01T00:00:00.004Z,d1,north,14,false,");

      // This older rewrite is issued after DELETE has completed and is protected by the SYNC WAL.
      statement.execute(
          "insert into restart_delete(time,device_id,site,s1) values(2,'d1','north',1202)");
      assertEquals(closedFiles, DeletionTestUtils.getClosedTsFiles("test", true));
      assertTrue(DeletionTestUtils.getClosedTsFiles("test", false).isEmpty());
      assertRestartTableData(
          statement,
          "1970-01-01T00:00:00.002Z,d1,north,1202,null,",
          "1970-01-01T00:00:00.004Z,d1,north,14,false,");
    }

    TestUtils.stopForciblyAndRestartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      connection.setClientInfo("time_zone", "+00:00");
      statement.execute("use \"test\"");
      assertRestartTableData(
          statement,
          "1970-01-01T00:00:00.002Z,d1,north,1202,null,",
          "1970-01-01T00:00:00.004Z,d1,north,14,false,");
      // Full data deletion must also preserve the other identities sharing the original file.
      statement.execute("delete from restart_delete where device_id='d1' and site='north'");
      assertRestartTableData(statement);
      statement.execute(
          "insert into restart_delete(time,device_id,site,s1) values(1,'d1','north',1101)");
      statement.execute("flush");
    }

    TestUtils.stopForciblyAndRestartDataNodes();
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      connection.setClientInfo("time_zone", "+00:00");
      statement.execute("use test");
      assertRestartTableData(statement, "1970-01-01T00:00:00.001Z,d1,north,1101,null,");
      statement.execute(
          "insert into restart_delete(time,device_id,site,s1,s2) values(5,'d1','north',15,true)");
      statement.execute("flush");
      assertRestartTableData(
          statement,
          "1970-01-01T00:00:00.001Z,d1,north,1101,null,",
          "1970-01-01T00:00:00.005Z,d1,north,15,true,");
    }
  }

  private static void assertRestartTableData(Statement statement, String... targetRows)
      throws SQLException {
    List<String> expected =
        new ArrayList<>(
            Arrays.asList(
                "1970-01-01T00:00:00.002Z,d1,null,52,true,",
                "1970-01-01T00:00:00.002Z,d1,,62,false,"));
    expected.addAll(Arrays.asList(targetRows));
    expected.add("1970-01-01T00:00:00.002Z,d1,south,212,true,");
    try (ResultSet rows =
        statement.executeQuery(
            "select time,device_id,site,s1,s2 from restart_delete "
                + "order by device_id,site nulls first,time")) {
      TestUtils.assertResultSetEqual(
          rows, "time,device_id,site,s1,s2,", expected.toArray(new String[0]));
    }
    try (ResultSet rows =
        statement.executeQuery(
            "select time,device_id,site,s1,s2 from restart_keep order by device_id,site,time")) {
      TestUtils.assertResultSetEqual(
          rows,
          "time,device_id,site,s1,s2,",
          new String[] {
            "1970-01-01T00:00:00.001Z,d1,north,311,true,",
            "1970-01-01T00:00:00.002Z,d1,north,312,false,"
          });
    }
  }

  @Test
  public void testRestartQueryLargerThanEndTime() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database \"test\"");
      statement.execute("use \"test\"");
      statement.execute("create table turbine (tag1 string tag, s1 int64 field, s2 boolean field)");
      statement.execute("insert into turbine(time,tag1,s1) values(1,\'d1\',1)");
      statement.execute("insert into turbine(time,tag1,s1) values(2,\'d1\',2)");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute("insert into turbine(time,tag1,s1) values(3,\'d1\',1)");
      statement.execute("insert into turbine(time,tag1,s1) values(4,\'d1\',2)");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");

      String[] exp =
          new String[] {
            "4,2",
          };
      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT Time, s1 FROM turbine where time > 3 and tag1=\'d1\'")) {
        assertNotNull(resultSet);
        while (resultSet.next()) {
          String result = resultSet.getLong(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
      assertEquals(1, cnt);
    }
  }

  @Test
  public void testRestartEndTime() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database \"test\"");
      statement.execute("use \"test\"");
      statement.execute("create table turbine (tag1 string tag, s1 int64 field, s2 int64 field)");
      statement.execute("insert into turbine(time,\"tag1\",s1) values(1,\'d1\',1)");
      statement.execute("insert into turbine(time,\"tag1\",s1) values(2,\'d1\',2)");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute("insert into turbine(time,tag1,s2) values(1,\'d1\',1)");
      statement.execute("insert into turbine(time,tag1,s2) values(2,\'d1\',2)");
    }

    try {
      TestUtils.restartDataNodes();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");

      String[] exp = new String[] {"1,1", "2,2"};
      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT Time, s2 FROM turbine where tag1=\'d1\'")) {
        assertNotNull(resultSet);
        while (resultSet.next()) {
          String result = resultSet.getLong(TIMESTAMP_STR) + "," + resultSet.getLong(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
      assertEquals(2, cnt);
    }
  }

  @Ignore // delete column
  @Test
  public void testRecoverWALMismatchDataType() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute("insert into root.turbine1.d1(time,s1,s2) values(1,1.1,2.2)");
      statement.execute("delete timeseries root.turbine1.d1.s1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT32, encoding=RLE, compression=SNAPPY");
    }

    TestUtils.restartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");

      try (ResultSet resultSet = statement.executeQuery("select * from root.turbine1.**")) {
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

  @Ignore // delete column
  @Test
  public void testRecoverWALDeleteSchema() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute("insert into turbine1(time,tag1,s1,s2) values(1,\'d1\',1.1,2.2)");
      statement.execute("delete timeseries root.turbine1.d1.s1");
    }

    TestUtils.restartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");

      try (ResultSet resultSet = statement.executeQuery("select * from root.turbine1.**")) {
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
    long tsFileSize = config.getSeqTsFileSize();
    long unFsFileSize = config.getSeqTsFileSize();
    config.setSeqTsFileSize(10000000);
    config.setUnSeqTsFileSize(10000000);

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database \"test\"");
      statement.execute("use \"test\"");
      statement.execute(
          "create table turbine1 (tag1 string tag, s1 int64 field, s2 boolean field)");
      statement.execute("insert into turbine1(time,tag1,s1) values(1,\'d1\',1)");
      statement.execute("insert into turbine1(time,tag1,s1) values(2,\'d1\',2)");
      statement.execute("insert into turbine1(time,tag1,s2) values(3,\'d1\',true)");
      statement.execute("insert into turbine1(time,tag1,s2) values(4,\'d1\',true)");
    }

    TestUtils.restartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");

      long[] result = new long[] {1L, 2L};
      ResultSet resultSet =
          statement.executeQuery("select s1 from turbine1 where time < 3 order by time");
      assertNotNull(resultSet);
      int cnt = 0;
      while (resultSet.next()) {
        assertEquals(result[cnt], resultSet.getLong(1));
        cnt++;
      }
      assertEquals(2, cnt);
    }

    config.setSeqTsFileSize(tsFileSize);
    config.setUnSeqTsFileSize(unFsFileSize);
  }

  @Test
  public void testRecoverFromFlushMemTableError() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database \"test\"");
      statement.execute("use \"test\"");
      statement.execute("create table turbine1 (tag1 string tag, s1 float field, s2 double field)");
      statement.execute("insert into turbine1(time,tag1,s1,s2) values(1,\'d1\',1.1,2.2)");
    }

    TestUtils.restartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");

      try (ResultSet resultSet = statement.executeQuery("select * from turbine1 order by time")) {
        assertNotNull(resultSet);
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(1, resultSet.getLong("time"));
          assertEquals("1.1", resultSet.getString("s1"));
          assertEquals("2.2", resultSet.getString("s2"));
          cnt++;
        }
        assertEquals(1, cnt);
      }
    }
  }
}
