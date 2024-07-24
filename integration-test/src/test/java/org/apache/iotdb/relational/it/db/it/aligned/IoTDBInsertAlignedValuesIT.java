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

package org.apache.iotdb.relational.it.db.it.aligned;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBInsertAlignedValuesIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInsertAlignedValues() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.addBatch("create database t1");
      statement.addBatch("use \"t1\"");
      statement.addBatch(
          "create table wf01 (id1 string id, status int32 measurement, temperature float measurement)");
      statement.addBatch(
          "insert into wf01(id1, time, status, temperature) values ('wt01', 4000, true, 17.1)");
      statement.addBatch(
          "insert into wf01(id1, time, status, temperature) values ('wt01', 5000, true, 20.1)");
      statement.addBatch(
          "insert into wf01(id1, time, status, temperature) values ('wt01', 6000, true, 22)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select time, status from wf01")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select time, status, temperature from wf01")) {

        assertTrue(resultSet.next());
        assertEquals(4000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(17.1, resultSet.getDouble(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(5000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(20.1, resultSet.getDouble(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(6000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(22, resultSet.getDouble(3), 0.1);

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertAlignedNullableValues() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.addBatch("create database t1");
      statement.addBatch("use \"t1\"");
      statement.addBatch(
          "create table wf01 (id1 string id, status boolean measurement, temperature float measurement)");
      statement.addBatch(
          "insert into wf01(id1, time, status, temperature) values ('wt01', 4000, true, 17.1)");
      statement.addBatch("insert into wf01(id1, time, status) values ('wt01', 5000, true)");
      statement.addBatch("insert into wf01(id1, time, temperature) values ('wt01', 6000, 22)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select status from wf01")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select time, status, temperature from wf01")) {

        assertTrue(resultSet.next());
        assertEquals(4000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(17.1, resultSet.getDouble(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(5000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertNull(resultSet.getObject(3));

        assertTrue(resultSet.next());
        assertEquals(6000, resultSet.getLong(1));
        assertNull(resultSet.getObject(2));
        assertEquals(22.0d, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testUpdatingAlignedValues() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.addBatch("create database t1");
      statement.addBatch("use \"t1\"");
      statement.addBatch(
          "create table wf01 (id1 string id, status boolean measurement, temperature float measurement)");
      statement.addBatch(
          "insert into wf01(id1, time, status, temperature) values ('wt01', 4000, true, 17.1)");
      statement.addBatch("insert into wf01(id1, time, status) values ('wt01', 5000, true)");
      statement.addBatch("insert into wf01(id1, time, temperature)values ('wt01', 5000, 20.1)");
      statement.addBatch("insert into wf01(id1, time, temperature)values ('wt01', 6000, 22)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select time, status from wf01")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select time, status, temperature from wf01")) {

        assertTrue(resultSet.next());
        assertEquals(4000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(17.1, resultSet.getDouble(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(5000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(20.1, resultSet.getDouble(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(6000, resultSet.getLong(1));
        assertNull(resultSet.getObject(2));
        assertEquals(22.0d, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }

      statement.execute("flush");
      try (ResultSet resultSet = statement.executeQuery("select time, status from wf01")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select time, status, temperature from wf01")) {

        assertTrue(resultSet.next());
        assertEquals(4000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(17.1, resultSet.getDouble(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(5000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(20.1, resultSet.getDouble(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(6000, resultSet.getLong(1));
        assertNull(resultSet.getObject(2));
        assertEquals(22.0d, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertAlignedValuesWithSameTimestamp() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.addBatch("create database test");
      statement.addBatch("use \"test\"");
      statement.addBatch(
          "create table sg (ids string id, s2 int32 measurement, s1 int32 measurement)");
      statement.addBatch("insert into sg(id1,time,s2) values('d1',1,2)");
      statement.addBatch("insert into sg(id1,time,s1) values('d1',1,2)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select time, s1, s2 from sg")) {

        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertEquals(2.0d, resultSet.getObject(2));
        assertEquals(2.0d, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }

      statement.execute("flush");
      try (ResultSet resultSet = statement.executeQuery("select time, s1, s2 from sg")) {

        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertEquals(2.0d, resultSet.getObject(2));
        assertEquals(2.0d, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertWithWrongMeasurementNum1() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database t1");
      statement.execute("use \"t1\"");
      statement.execute(
          "create table wf01 (id1 string id, status int32, temperature int32 measurement)");
      statement.execute(
          "insert into wf01(id1, time, status, temperature) values('wt01', 11000, 100)");
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("failed"));
    }
  }

  @Test
  public void testInsertWithWrongMeasurementNum2() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database t1");
      statement.execute("use \"t1\"");
      statement.execute(
          "create table wf01 (id1 string id, status int32, temperature int32 measurement)");
      statement.execute(
          "insert into wf01(id1, time, status, temperature) values('wt01', 11000, 100, 300, 400)");
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("failed"));
    }
  }

  @Test(expected = Exception.class)
  public void testInsertWithWrongType() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database lz");
      statement.execute("use \"lz\"");
      statement.execute(
          "create table dev (id1 string id, latitude int32 measurement, longitude int32 measurement)");
      statement.execute("insert into dev(id1,time,latitude,longitude) values('GPS', 1,1.3,6.7)");
      fail();
    }
  }

  @Test
  @Ignore // TODO: delete
  public void testInsertAlignedTimeseriesWithoutAligned() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.lz.dev.GPS2(latitude INT32 encoding=PLAIN compressor=SNAPPY, longitude INT32 encoding=PLAIN compressor=SNAPPY) ");
      statement.execute("insert into root.lz.dev.GPS2(time,latitude,longitude) values(1,123,456)");
      // it's supported.
    }
  }

  @Test
  public void testInsertTimeseriesWithUnMatchedAlignedType() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create ALIGNED timeseries root.db.d_aligned(s01 INT64 encoding=RLE)");
      statement.execute("insert into root.db.d_aligned(time, s01) aligned values (4000, 123)");
      statement.execute("insert into root.db.d_aligned(time, s01) values (5000, 456)");
      statement.execute("create timeseries root.db.d_not_aligned.s01 INT64 encoding=RLE");
      statement.execute("insert into root.db.d_not_aligned(time, s01) values (4000, 987)");
      statement.execute("insert into root.db.d_not_aligned(time, s01) aligned values (5000, 654)");

      try (ResultSet resultSet = statement.executeQuery("select s01 from root.db.d_aligned")) {
        assertTrue(resultSet.next());
        assertEquals(4000, resultSet.getLong(1));
        assertEquals(123, resultSet.getLong(2));

        assertTrue(resultSet.next());
        assertEquals(5000, resultSet.getLong(1));
        assertEquals(456, resultSet.getLong(2));

        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = statement.executeQuery("select s01 from root.db.d_not_aligned")) {
        assertTrue(resultSet.next());
        assertEquals(4000, resultSet.getLong(1));
        assertEquals(987, resultSet.getLong(2));

        assertTrue(resultSet.next());
        assertEquals(5000, resultSet.getLong(1));
        assertEquals(654, resultSet.getLong(2));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  @Ignore // TODO: delete
  public void testInsertNonAlignedTimeseriesWithAligned() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.lz.dev.GPS3.latitude with datatype=INT32");
      statement.execute("CREATE TIMESERIES root.lz.dev.GPS3.longitude with datatype=INT32");
      statement.execute(
          "insert into root.lz.dev.GPS3(time,latitude,longitude) aligned values(1,123,456)");
      // it's supported.
    }
  }

  @Test
  @Ignore // TODO: delete
  public void testInsertAlignedValuesWithThreeLevelPath() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg_device(time, status) aligned values (4000, true)");

      try (ResultSet resultSet = statement.executeQuery("select ** from root")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertWithDuplicatedMeasurements() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database t1");
      statement.execute("use \"t1\"");
      statement.execute("create table wf01(id1 string id, s3 boolean measurement, status int32)");
      statement.execute(
          "insert into wf01(id1, time, s3, status, status) values('wt01', 100, true, 20.1, 20.2)");
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage(),
          e.getMessage().contains("Insertion contains duplicated measurement: status"));
    }
  }

  @Test
  public void testInsertMultiRows() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database test");
      statement.execute("use \"test\"");
      statement.execute(
          "create table sg1 (id1 string id, s1 int32 measurement, s2 int32 measurement)");
      statement.execute(
          "insert into sg1(id1, time, s1, s2) values('d1', 10, 2, 2), ('d1', 11, 3, '3'), ('d1', 12,12.11,false)");
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("data type is not consistent"));
    }
  }

  @Test
  public void testInsertLargeNumber() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database test");
      statement.execute("use \"test\"");
      statement.execute(
          "create table sg1 (id1 string id, s98 int64 measurement, s99 int64 measurement)");
      statement.execute(
          "insert into sg1(id1, time, s98, s99) values('d1', 10, 2, 271840880000000000000000)");
    } catch (SQLException e) {
      fail();
    }
  }
}
