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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
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
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBInsertAlignedValuesTableIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database t1");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInsertAlignedValues() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.addBatch(
          "create table wf01 (id1 string id, status boolean measurement, temperature float measurement)");
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

      statement.execute("use \"t1\"");
      statement.addBatch(
          "create table wf02 (id1 string id, status boolean measurement, temperature float measurement)");
      statement.addBatch(
          "insert into wf02(id1, time, status, temperature) values ('wt01', 4000, true, 17.1)");
      statement.addBatch("insert into wf02(id1, time, status) values ('wt01', 5000, true)");
      statement.addBatch("insert into wf02(id1, time, temperature) values ('wt01', 6000, 22)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select status from wf02")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
        assertTrue(resultSet.next());
        resultSet.getBoolean(1);
        assertTrue(resultSet.wasNull());
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select time, status, temperature from wf02")) {

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
        assertEquals(22.0f, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testUpdatingAlignedValues() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.addBatch(
          "create table wf03 (id1 string id, status boolean measurement, temperature float measurement)");
      statement.addBatch(
          "insert into wf03(id1, time, status, temperature) values ('wt01', 4000, true, 17.1)");
      statement.addBatch("insert into wf03(id1, time, status) values ('wt01', 5000, true)");
      statement.addBatch("insert into wf03(id1, time, temperature)values ('wt01', 5000, 20.1)");
      statement.addBatch("insert into wf03(id1, time, temperature)values ('wt01', 6000, 22)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select time, status from wf03")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        resultSet.getBoolean(2);
        assertTrue(resultSet.wasNull());
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select time, status, temperature from wf03")) {

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
        assertEquals(22.0f, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }

      statement.execute("flush");
      try (ResultSet resultSet = statement.executeQuery("select time, status from wf03")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(2));
        assertTrue(resultSet.next());
        resultSet.getBoolean(2);
        assertTrue(resultSet.wasNull());
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select time, status, temperature from wf03")) {

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
        assertEquals(22.0f, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertAlignedValuesWithSameTimestamp() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.addBatch(
          "create table sg3 (id1 string id, s2 double measurement, s1 double measurement)");
      statement.addBatch("insert into sg3(id1,time,s2) values('d1',1,2)");
      statement.addBatch("insert into sg3(id1,time,s1) values('d1',1,2)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select time, s1, s2 from sg3")) {

        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertEquals(2.0d, resultSet.getObject(2));
        assertEquals(2.0d, resultSet.getObject(3));

        assertFalse(resultSet.next());
      }

      statement.execute("flush");
      try (ResultSet resultSet = statement.executeQuery("select time, s1, s2 from sg3")) {

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
      statement.execute("use \"t1\"");
      statement.execute(
          "create table wf04 (id1 string id, status int32, temperature int32 measurement)");
      statement.execute(
          "insert into wf04(id1, time, status, temperature) values('wt01', 11000, 100)");
      fail();
    } catch (SQLException e) {
      assertEquals(
          "701: Inconsistent numbers of non-time column names and values: 3-2", e.getMessage());
    }
  }

  @Test
  public void testInsertWithWrongMeasurementNum2() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.execute(
          "create table wf05 (id1 string id, status int32, temperature int32 measurement)");
      statement.execute(
          "insert into wf05(id1, time, status, temperature) values('wt01', 11000, 100, 300, 400)");
      fail();
    } catch (SQLException e) {
      assertEquals(
          "701: Inconsistent numbers of non-time column names and values: 3-4", e.getMessage());
    }
  }

  @Test(expected = Exception.class)
  public void testInsertWithWrongType() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.execute(
          "create table dev6 (id1 string id, latitude int32 measurement, longitude int32 measurement)");
      statement.execute("insert into dev6(id1,time,latitude,longitude) values('GPS', 1,1.3,6.7)");
      fail();
    }
  }

  @Test
  public void testInsertWithDuplicatedMeasurements() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.execute("create table wf07(id1 string id, s3 boolean measurement, status int32)");
      statement.execute(
          "insert into wf07(id1, time, s3, status, status) values('wt01', 100, true, 20.1, 20.2)");
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
      statement.execute("use \"t1\"");
      statement.execute(
          "create table sg8 (id1 string id, s1 int32 measurement, s2 int32 measurement)");
      statement.execute(
          "insert into sg8(id1, time, s1, s2) values('d1', 10, 2, 2), ('d1', 11, 3, '3'), ('d1', 12,12.11,false)");
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("data type is not consistent"));
    }
  }

  @Test
  public void testInsertLargeNumber() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.execute(
          "create table sg9 (id1 string id, s98 int64 measurement, s99 int64 measurement)");
      statement.execute(
          "insert into sg9(id1, time, s98, s99) values('d1', 10, 2, 271840880000000000000000)");
      fail("Exception expected");
    } catch (SQLException e) {
      assertEquals(
          "700: line 1:58: Invalid numeric literal: 271840880000000000000000", e.getMessage());
    }
  }

  @Test
  public void testInsertAlignedWithEmptyPage() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.execute(
          "create table dev10 (id1 string id, s1 int32 measurement, s2 int32 measurement, s3 int32 measurement)");
      for (int i = 0; i < 100; i++) {
        if (i == 99) {
          statement.addBatch(
              "insert into dev10(id1,time,s1,s3) values("
                  + "'GPS'"
                  + ","
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        } else {
          statement.addBatch(
              "insert into dev10(id1, time,s1,s2) values("
                  + "'GPS'"
                  + ","
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        }
      }
      statement.executeBatch();

      statement.execute("flush");
      int rowCount = 0;
      try (ResultSet resultSet = statement.executeQuery("select time, s3 from dev10")) {
        while (resultSet.next()) {
          int v = resultSet.getInt(2);
          if (rowCount == 99) {
            assertEquals(99, v);
          } else {
            assertTrue(resultSet.wasNull());
          }
          rowCount++;
        }
        assertEquals(100, rowCount);
      }

      try (ResultSet resultSet = statement.executeQuery("select time, s2 from dev10")) {
        rowCount = 0;
        while (resultSet.next()) {
          int v = resultSet.getInt(2);
          if (rowCount == 99) {
            assertTrue(resultSet.wasNull());
          } else {
            assertEquals(rowCount, v);
          }
          rowCount++;
        }
        assertEquals(100, rowCount);
      }

      try (ResultSet resultSet = statement.executeQuery("select time, s1 from dev10")) {
        rowCount = 0;
        while (resultSet.next()) {
          assertEquals(rowCount, resultSet.getInt(2));
          rowCount++;
        }
        assertEquals(100, rowCount);
      }
    }
  }

  @Test
  public void testInsertAlignedWithEmptyPage2() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.execute(
          "create table sg11 (id1 string id, s1 string measurement, s2 string measurement)");

      statement.execute("insert into sg11(id1, time, s1, s2) values('d1', 1,'aa','bb')");
      statement.execute("insert into sg11(id1, time, s1, s2) values('d1', 1,'aa','bb')");
      statement.execute("insert into sg11(id1, time, s1, s2) values('d2', 1,'aa','bb')");
      statement.execute("flush");
      statement.execute("insert into sg11(id1, time, s1, s2) values('d1', 1,'aa','bb')");
    }
  }

  @Ignore // aggregation
  @Test
  public void testInsertComplexAlignedValues() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.addBatch(
          "create table sg12 (id1 string id, s1 int32 measurement, s2 int32 measurement)");
      statement.addBatch("insert into sg12(id1, time, s1) values('id1', 3,1)");
      statement.addBatch("insert into sg12(id1, time, s1) values('id1', 3,1)");
      statement.addBatch("insert into sg12(id1, time, s1) values('id1', 1,1)");
      statement.addBatch("insert into sg12(id1, time, s1) values('id1', 2,1)");
      statement.addBatch("insert into sg12(id1, time, s2) values('id1', 2,2)");
      statement.addBatch("insert into sg12(id1, time, s2) values('id1', 1,2)");
      statement.addBatch("insert into sg12(id1, time, s2) values('id1', 3,2)");
      statement.addBatch("insert into sg12(id1, time, s3) values('id1', 1,3)");
      statement.addBatch("insert into sg12(id1, time, s3) values('id1', 3,3)");
      statement.executeBatch();

      try (ResultSet resultSet =
          statement.executeQuery("select count(s1), count(s2), count(s3) from sg12")) {

        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getInt(1));
        assertEquals(3, resultSet.getInt(2));
        assertEquals(2, resultSet.getInt(3));

        assertFalse(resultSet.next());
      }

      statement.execute("flush");
      try (ResultSet resultSet =
          statement.executeQuery("select count(s1), count(s2), count(s3) from sg12")) {

        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getInt(1));
        assertEquals(3, resultSet.getInt(2));
        assertEquals(2, resultSet.getInt(3));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertAlignedWithEmptyPage3() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.execute(
          "create table dev13 (id1 string id, s1 int32 measurement, s2 int32 measurement, s3 int32 measurement)");
      for (int i = 0; i < 100; i++) {
        if (i >= 49) {
          statement.addBatch(
              "insert into dev13(id1,time,s1,s2,s3) values("
                  + "\'GPS\'"
                  + ","
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        } else {
          statement.addBatch(
              "insert into dev13(id1,time,s1,s2) values("
                  + "\'GPS\'"
                  + ","
                  + i
                  + ","
                  + i
                  + ","
                  + i
                  + ")");
        }
      }
      statement.executeBatch();
      statement.execute("flush");
      int rowCount = 0;
      try (ResultSet resultSet = statement.executeQuery("select s3 from dev13")) {
        while (resultSet.next()) {
          int v = resultSet.getInt(1);
          if (rowCount >= 49) {
            assertEquals(rowCount, v);
          } else {
            assertTrue(resultSet.wasNull());
          }
          rowCount++;
        }
        assertEquals(100, rowCount);
      }

      try (ResultSet resultSet = statement.executeQuery("select s2 from dev13")) {
        rowCount = 0;
        while (resultSet.next()) {
          assertEquals(rowCount, resultSet.getInt(1));
          rowCount++;
        }
        assertEquals(100, rowCount);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from dev13")) {
        rowCount = 0;
        while (resultSet.next()) {
          assertEquals(rowCount, resultSet.getInt(1));
          rowCount++;
        }
        assertEquals(100, rowCount);
      }
    }
  }

  @Test
  public void testExtendTextColumn() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"t1\"");
      statement.execute(
          "create table sg14 (id1 string id, s1 string measurement, s2 string measurement)");
      statement.execute("insert into sg14(id1,time,s1,s2) values('d1',1,'test','test')");
      statement.execute("insert into sg14(id1,time,s1,s2) values('d1',2,'test','test')");
      statement.execute("insert into sg14(id1,time,s1,s2) values('d1',3,'test','test')");
      statement.execute("insert into sg14(id1,time,s1,s2) values('d1',4,'test','test')");
      statement.execute("insert into sg14(id1,time,s1,s3) values('d1',5,'test','test')");
      statement.execute("insert into sg14(id1,time,s1,s2) values('d1',6,'test','test')");
      statement.execute("flush");
      statement.execute("insert into sg14(id1,time,s1,s3) values('d1',7,'test','test')");
      fail();
    } catch (SQLException ignored) {
    }
  }
}
