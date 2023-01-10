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

package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch(
          "insert into root.t1.wf01.wt01(time, status, temperature) aligned values (4000, true, 17.1)");
      statement.addBatch(
          "insert into root.t1.wf01.wt01(time, status, temperature) aligned values (5000, true, 20.1)");
      statement.addBatch(
          "insert into root.t1.wf01.wt01(time, status, temperature) aligned values (6000, true, 22)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select status from root.t1.wf01.wt01")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select status, temperature from root.t1.wf01.wt01")) {

        assertTrue(resultSet.next());
        assertEquals(4000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(17.1, resultSet.getFloat(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(5000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(20.1, resultSet.getFloat(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(6000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(22, resultSet.getFloat(3), 0.1);

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertAlignedNullableValues() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch(
          "insert into root.t1.wf01.wt01(time, status, temperature) aligned values (4000, true, 17.1)");
      statement.addBatch("insert into root.t1.wf01.wt01(time, status) aligned values (5000, true)");
      statement.addBatch(
          "insert into root.t1.wf01.wt01(time, temperature) aligned values (6000, 22)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select status from root.t1.wf01.wt01")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select status, temperature from root.t1.wf01.wt01")) {

        assertTrue(resultSet.next());
        assertEquals(4000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(17.1, resultSet.getFloat(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(5000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertNull(resultSet.getObject(3));

        assertTrue(resultSet.next());
        assertEquals(6000, resultSet.getLong(1));
        assertNull(resultSet.getObject(2));
        assertEquals(22.0f, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testUpdatingAlignedValues() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch(
          "insert into root.t1.wf01.wt01(time, status, temperature) aligned values (4000, true, 17.1)");
      statement.addBatch("insert into root.t1.wf01.wt01(time, status) aligned values (5000, true)");
      statement.addBatch(
          "insert into root.t1.wf01.wt01(time, temperature) aligned values (5000, 20.1)");
      statement.addBatch(
          "insert into root.t1.wf01.wt01(time, temperature) aligned values (6000, 22)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select status from root.t1.wf01.wt01")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select status, temperature from root.t1.wf01.wt01")) {

        assertTrue(resultSet.next());
        assertEquals(4000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(17.1, resultSet.getFloat(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(5000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(20.1, resultSet.getFloat(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(6000, resultSet.getLong(1));
        assertNull(resultSet.getObject(2));
        assertEquals(22.0f, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }

      statement.execute("flush");
      try (ResultSet resultSet = statement.executeQuery("select status from root.t1.wf01.wt01")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select status, temperature from root.t1.wf01.wt01")) {

        assertTrue(resultSet.next());
        assertEquals(4000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(17.1, resultSet.getFloat(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(5000, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertEquals(20.1, resultSet.getFloat(3), 0.1);

        assertTrue(resultSet.next());
        assertEquals(6000, resultSet.getLong(1));
        assertNull(resultSet.getObject(2));
        assertEquals(22.0f, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertWithWrongMeasurementNum1() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "insert into root.t1.wf01.wt01(time, status, temperature) aligned values(11000, 100)");
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "the measurementList's size 2 is not consistent with the valueList's size 1"));
    }
  }

  @Test
  public void testInsertWithWrongMeasurementNum2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "insert into root.t1.wf01.wt01(time, status, temperature) aligned values(11000, 100, 300, 400)");
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage(),
          e.getMessage()
              .contains(
                  "the measurementList's size 2 is not consistent with the valueList's size 3"));
    }
  }

  @Test(expected = Exception.class)
  public void testInsertWithWrongType() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.lz.dev.GPS(latitude INT32 encoding=PLAIN compressor=SNAPPY, longitude INT32 encoding=PLAIN compressor=SNAPPY) ");
      statement.execute(
          "insert into root.lz.dev.GPS(time,latitude,longitude) aligned values(1,1.3,6.7)");
      fail();
    }
  }

  @Test
  public void testInsertAlignedTimeseriesWithoutAligned() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.lz.dev.GPS2(latitude INT32 encoding=PLAIN compressor=SNAPPY, longitude INT32 encoding=PLAIN compressor=SNAPPY) ");
      statement.execute("insert into root.lz.dev.GPS2(time,latitude,longitude) values(1,1.3,6.7)");
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage(),
          e.getMessage()
              .contains("timeseries under this device are aligned, please use aligned interface"));
    }
  }

  @Test
  public void testInsertNonAlignedTimeseriesWithAligned() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.lz.dev.GPS3.latitude with datatype=INT32");
      statement.execute("CREATE TIMESERIES root.lz.dev.GPS3.longitude with datatype=INT32");
      statement.execute(
          "insert into root.lz.dev.GPS3(time,latitude,longitude) aligned values(1,1.3,6.7)");
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage(),
          e.getMessage()
              .contains(
                  "timeseries under this device are not aligned, please use non-aligned interface"));
    }
  }

  @Test
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "insert into root.t1.wf01.wt01(time, s3, status, status) aligned values(100, true, 20.1, 20.2)");
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage(),
          e.getMessage().contains("Insertion contains duplicated measurement: status"));
    }
  }

  @Test
  public void testInsertMultiRows() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "insert into root.sg1.d1(time, s1, s2) aligned values(10, 2, 2), (11, 3, '3'), (12,12.11,false);");
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("data type is not consistent"));
    }
  }

  @Test
  public void testInsertLargeNumber() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "insert into root.sg1.d1(time, s98, s99) aligned values(10, 2, 271840880000000000000000)");
    } catch (SQLException e) {
      fail();
    }
  }
}
