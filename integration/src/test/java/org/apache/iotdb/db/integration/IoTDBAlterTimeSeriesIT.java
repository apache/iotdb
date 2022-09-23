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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBAlterTimeSeriesIT {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBAlterTimeSeriesIT.class);
  private long prevPartitionInterval;

  @Before
  public void setUp() throws Exception {
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    ConfigFactory.getConfig().setPartitionInterval(1);
    EnvFactory.getEnv().initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
    ConfigFactory.getConfig().setPartitionInterval(prevPartitionInterval);
  }

  @Test
  public void testAlter() throws SQLException {
    logger.info("test...");
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.alterTimeSeriesTest");
      try {
        statement.execute(
            "CREATE TIMESERIES root.alterTimeSeriesTest.s1 WITH DATATYPE=INT64,ENCODING=PLAIN");
        statement.execute(
            "CREATE TIMESERIES root.alterTimeSeriesTest.s2 WITH DATATYPE=INT64,ENCODING=PLAIN");
      } catch (SQLException e) {
        // ignore
      }

      for (int i = 1; i <= 1000; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.alterTimeSeriesTest(timestamp,s1,s2) VALUES (%d,%d,%d)",
                i, i, i));
      }
      statement.execute("FLUSH");
      ResultSet resultSetP = statement.executeQuery("show timeseries root.alterTimeSeriesTest.s1");
      while (resultSetP.next()) {
        assertEquals(resultSetP.getString("encoding"), "PLAIN");
        assertEquals(resultSetP.getString("compression"), "SNAPPY");
      }
      statement.execute(
          "alter timeseries root.alterTimeSeriesTest.s1 settype encoding=gorilla,compression=gzip");

      ResultSet resultSetAL = statement.executeQuery("show timeseries root.alterTimeSeriesTest.s1");
      while (resultSetAL.next()) {
        assertEquals(resultSetAL.getString("encoding"), "GORILLA");
        assertEquals(resultSetAL.getString("compression"), "GZIP");
      }
      for (int i = 1001; i <= 1010; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.alterTimeSeriesTest(timestamp,s1,s2) VALUES (%d,%d,%d)",
                i, i, i));
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.alterTimeSeriesTest")) {
        int cur = 1;
        int count = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.alterTimeSeriesTest.s1");
          long s2 = resultSet.getLong("root.alterTimeSeriesTest.s2");

          assertEquals(cur, time);
          assertEquals(cur, s1);
          assertEquals(cur, s2);
          cur++;
          count++;
        }
        assertEquals(1010, count);
      }

      statement.execute("REWRITE TIMESERIES root.alterTimeSeriesTest");
    }
  }
}
