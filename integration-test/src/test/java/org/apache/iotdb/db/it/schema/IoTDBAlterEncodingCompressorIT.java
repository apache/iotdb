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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlterEncodingCompressorIT extends AbstractSchemaIT {

  public IoTDBAlterEncodingCompressorIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  @Test
  public void alterEncodingAndCompressorTest() throws Exception {
    if (schemaTestMode.equals(SchemaTestMode.PBTree)) {
      return;
    }
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create timeSeries root.vehicle.wind.a int32");

      try {
        statement.execute("alter timeSeries root set STORAGE_PROPERTIES encoding=PLAIN");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals("701: The timeSeries shall not be root.", e.getMessage());
      }

      try {
        statement.execute(
            "alter timeSeries root.nonExist.** set STORAGE_PROPERTIES encoding=PLAIN");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals(
            "508: Timeseries [root.nonExist.**] does not exist or is represented by device template",
            e.getMessage());
      }

      try {
        statement.execute(
            "alter timeSeries if exists root.nonExist.** set STORAGE_PROPERTIES encoding=PLAIN");
      } catch (final SQLException e) {
        fail(
            "Alter encoding & compressor shall not fail when timeSeries not exists if set if exists");
      }

      try {
        statement.execute(
            "alter timeSeries if exists root.vehicle.** set STORAGE_PROPERTIES encoding=aaa");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals("701: Unsupported encoding: AAA", e.getMessage());
      }

      try {
        statement.execute(
            "alter timeSeries if exists root.vehicle.** set STORAGE_PROPERTIES compressor=aaa");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals("701: Unsupported compressor: AAA", e.getMessage());
      }

      try {
        statement.execute(
            "alter timeSeries if exists root.vehicle.** set STORAGE_PROPERTIES falseKey=aaa");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals("701: property falsekey is unsupported yet.", e.getMessage());
      }

      try {
        statement.execute(
            "alter timeSeries if exists root.vehicle.** set STORAGE_PROPERTIES encoding=DICTIONARY");
        fail();
      } catch (final SQLException e) {
        Assert.assertTrue(e.getMessage().contains("encoding DICTIONARY does not support INT32"));
      }

      statement.execute(
          "alter timeSeries root.** set STORAGE_PROPERTIES encoding=Plain, compressor=LZMA2");

      try (final ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES")) {
        while (resultSet.next()) {
          assertEquals("PLAIN", resultSet.getString(5));
          assertEquals("LZMA2", resultSet.getString(6));
        }
      }

      statement.execute("create user IoTDBUser '!@#$!dfdfzvd343'");
      statement.execute("grant write on root.vehicle.wind.a to user IoTDBUser");
      statement.execute("create timeSeries root.vehicle.wind.b int32");
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection("IoTDBUser", "!@#$!dfdfzvd343");
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "alter timeSeries root.vehicle.** set STORAGE_PROPERTIES encoding=PLAIN, compressor=LZMA2");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals(
            "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.vehicle.**]",
            e.getMessage());
      }

      try {
        statement.execute(
            "alter timeSeries root.vehicle.wind.a, root.__audit.** set STORAGE_PROPERTIES encoding=PLAIN, compressor=LZMA2");
        fail();
      } catch (final SQLException e) {
        Assert.assertEquals(
            "803: 'AUDIT' permission is needed to alter the encoding and compressor of database root.__audit",
            e.getMessage());
      }

      try {
        statement.execute(
            "alter timeSeries if permitted root.vehicle.**, root.__audit.** set STORAGE_PROPERTIES encoding=GORILLA, compressor=GZIP");
      } catch (final SQLException e) {
        fail("Alter encoding & compressor shall not fail when no privileges if set if permitted");
      }

      try {
        statement.execute(
            "alter timeSeries if permitted root.nonExist.** set STORAGE_PROPERTIES encoding=GORILLA, compressor=GZIP");
      } catch (final SQLException e) {
        fail("Alter encoding & compressor shall not fail if the intersected paths are empty");
      }
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      try (final ResultSet resultSet =
          statement.executeQuery("SHOW TIMESERIES root.__audit.**._0.password")) {
        while (resultSet.next()) {
          assertEquals("PLAIN", resultSet.getString(5));
          assertEquals("LZMA2", resultSet.getString(6));
        }
      }

      try (final ResultSet resultSet =
          statement.executeQuery("SHOW TIMESERIES root.vehicle.wind.b")) {
        resultSet.next();
        assertEquals("TS_2DIFF", resultSet.getString(5));
        assertEquals("LZ4", resultSet.getString(6));
      }

      try (final ResultSet resultSet =
          statement.executeQuery("SHOW TIMESERIES root.vehicle.wind.a")) {
        resultSet.next();
        assertEquals("GORILLA", resultSet.getString(5));
        assertEquals("GZIP", resultSet.getString(6));
      }
    }
  }
}
