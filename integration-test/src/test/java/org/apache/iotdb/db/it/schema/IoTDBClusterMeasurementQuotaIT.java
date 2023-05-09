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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class IoTDBClusterMeasurementQuotaIT extends AbstractSchemaIT {
  public IoTDBClusterMeasurementQuotaIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    setUpQuotaConfig();
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTimeSeries();
    Thread.sleep(2000); // wait heartbeat
  }

  protected void setUpQuotaConfig() {
    EnvFactory.getEnv().getConfig().getCommonConfig().setClusterSchemaLimitLevel("MEASUREMENT");
    EnvFactory.getEnv().getConfig().getCommonConfig().setClusterMaxSchemaCount(6);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    super.tearDown();
  }

  /** Prepare time series: 2 databases, 3 devices, 6 time series */
  private void prepareTimeSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // create database
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("CREATE DATABASE root.sg2");
      // create schema template
      statement.execute("CREATE SCHEMA TEMPLATE t1 (s1 INT64, s2 DOUBLE)");
      // set schema template
      statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg2");
      statement.execute(
          "create timeseries root.sg1.d0.s0 with datatype=FLOAT, encoding=RLE, compression=SNAPPY");
      statement.execute(
          "create timeseries root.sg1.d0.s1 with datatype=FLOAT, encoding=RLE, compression=SNAPPY");
      statement.execute(
          "create timeseries root.sg1.d1.s0 with datatype=FLOAT, encoding=RLE, compression=SNAPPY");
      statement.execute(
          "create timeseries root.sg1.d1.s1 with datatype=FLOAT, encoding=RLE, compression=SNAPPY");
      statement.execute("create timeseries of schema template on root.sg2.d1;");
    }
  }

  @Test
  public void testClusterSchemaQuota() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create timeseries root.sg1.d3.s0 with datatype=FLOAT, encoding=RLE, compression=SNAPPY");
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("The current metadata capacity has exceeded the cluster quota"));
      }
      try {
        statement.execute("create timeseries of schema template on root.sg2.d2;");
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("The current metadata capacity has exceeded the cluster quota"));
      }
      // delete some timeseries and database
      statement.execute("delete database root.sg2;");
      statement.execute("delete timeseries root.sg1.d0.s0;");
      Thread.sleep(2000); // wait heartbeat
      // now we can create 3 new timeseries or 1 new device
      statement.execute("SET SCHEMA TEMPLATE t1 TO root.sg1.d4");
      statement.execute("create timeseries of schema template on root.sg1.d4");
      statement.execute(
          "create timeseries root.sg1.d4.s3 with datatype=FLOAT, encoding=RLE, compression=SNAPPY");
      Thread.sleep(2000); // wait heartbeat
      try {
        statement.execute(
            "create timeseries root.sg1.d3.s0 with datatype=FLOAT, encoding=RLE, compression=SNAPPY");
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("The current metadata capacity has exceeded the cluster quota"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
