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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBUDFManagementIT {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    IoTDB.metaManager.setStorageGroup("root.vehicle");
    IoTDB.metaManager.createTimeseries("root.vehicle.d1.s1", TSDataType.FLOAT, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    IoTDB.metaManager.createTimeseries("root.vehicle.d1.s2", TSDataType.FLOAT, TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED, null);
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCreateReflectShowDrop() {
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      statement.execute("select udf(*, *) from root.vehicle");
      ResultSet resultSet = statement.executeQuery("show functions");
      assertEquals(3, resultSet.getMetaData().getColumnCount());
      resultSet = statement.executeQuery("show temporary functions");
      assertEquals(3, resultSet.getMetaData().getColumnCount());
      statement.execute("drop function udf");
    } catch (SQLException throwable) {
      throwable.printStackTrace();
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCreateAndDropSeveralTimes() {
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      statement.execute("select udf(*, *) from root.vehicle");
      ResultSet resultSet = statement.executeQuery("show functions");
      assertEquals(3, resultSet.getMetaData().getColumnCount());
      resultSet = statement.executeQuery("show temporary functions");
      assertEquals(3, resultSet.getMetaData().getColumnCount());
      statement.execute("drop function udf");

      statement.execute(
          "create temporary function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      statement.execute("select udf(*, *) from root.vehicle");
      resultSet = statement.executeQuery("show functions");
      assertEquals(3, resultSet.getMetaData().getColumnCount());
      resultSet = statement.executeQuery("show temporary functions");
      assertEquals(3, resultSet.getMetaData().getColumnCount());
      statement.execute("drop function udf");

      statement.execute("create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      statement.execute("select udf(*, *) from root.vehicle");
      resultSet = statement.executeQuery("show functions");
      assertEquals(3, resultSet.getMetaData().getColumnCount());
      resultSet = statement.executeQuery("show temporary functions");
      assertEquals(3, resultSet.getMetaData().getColumnCount());
      statement.execute("drop function udf");
    } catch (SQLException throwable) {
      throwable.printStackTrace();
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testReflectBeforeCreate() {
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select udf(*, *) from root.vehicle");
    } catch (SQLException throwable) {
      throwable.printStackTrace();
      assertTrue(throwable.getMessage().contains("Failed to reflect UDF instance"));
    }
  }

  @Test
  public void testReflectAfterDrop() {
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      statement.execute("drop function udf");
      statement.execute("select udf(*, *) from root.vehicle");
    } catch (SQLException throwable) {
      throwable.printStackTrace();
      assertTrue(throwable.getMessage().contains("Failed to reflect UDF instance"));
    }
  }

  @Test
  public void testCreateFunction1() { // create function twice
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      statement.execute("create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("has already been registered successfully"));
    }
  }

  @Test
  public void testCreateFunction2() { // create function twice
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create temporary function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      statement.execute(
          "create temporary function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("has already been registered successfully"));
    }
  }

  @Test
  public void testCreateFunction3() { // create function twice
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create temporary function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      statement.execute("create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage()
          .contains("with the same function name and the class name has already been registered"));
    }
  }

  @Test
  public void testCreateFunction4() { // create function twice
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      statement.execute(
          "create temporary function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage()
          .contains("with the same function name and the class name has already been registered"));
    }
  }

  @Test
  public void testDropFunction1() { // create + drop twice
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      statement.execute("drop function udf");
      statement.execute("drop function udf");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("does not exist"));
    }
  }

  @Test
  public void testDropFunction2() { // drop
    try (Connection connection = DriverManager.getConnection(
        Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("drop function udf");
      fail();
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("does not exist"));
    }
  }
}
