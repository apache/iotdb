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

package org.apache.iotdb.cluster.integration;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SingleNodeTest extends BaseSingleNodeTest {

  private Session session;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    session = openSession();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void testInsertRecordsWithIllegalPath()
      throws StatementExecutionException, IoTDBConnectionException {
    List<String> deviceIds = Arrays.asList("root..ln1", "root.sg.ln1", "root..ln1", "root.sg3.ln1");
    List<Long> timestamps = Arrays.asList(3L, 3L, 3L, 3L);
    List<String> measurements = Arrays.asList("dev1", "dev2", "dev3");
    List<List<String>> allMeasurements =
        Arrays.asList(measurements, measurements, measurements, measurements);
    List<String> values = Arrays.asList("123", "333", "444");
    List<List<String>> allValues = Arrays.asList(values, values, values, values);
    try {
      session.insertRecords(deviceIds, timestamps, allMeasurements, allValues);
      fail("Exception expected");
    } catch (StatementExecutionException e) {
      assertTrue(e.getMessage().contains("root..ln1 is not a legal path"));
    }

    List<String> legalDevices = Arrays.asList("root.sg.ln1", "root.sg3.ln1");
    for (String legalDevice : legalDevices) {
      for (String measurement : measurements) {
        assertTrue(
            session.checkTimeseriesExists(
                legalDevice + IoTDBConstant.PATH_SEPARATOR + measurement));
      }
    }
  }

  @Test
  public void testDeleteNonExistTimeSeries()
      throws StatementExecutionException, IoTDBConnectionException {
    session.insertRecord(
        "root.sg1.d1", 0, Arrays.asList("t1", "t2", "t3"), Arrays.asList("123", "333", "444"));
    session.deleteTimeseries(Arrays.asList("root.sg1.d1.t6", "root.sg1.d1.t2", "root.sg1.d1.t3"));

    assertTrue(session.checkTimeseriesExists("root.sg1.d1.t1"));
    assertFalse(session.checkTimeseriesExists("root.sg1.d1.t2"));
    assertFalse(session.checkTimeseriesExists("root.sg1.d1.t3"));
  }

  @Test
  public void testUserPrivilege() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create user user1 '1234'");
      try (Connection connection1 =
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "user1", "1234");
          Statement userStatement = connection1.createStatement()) {
        userStatement.addBatch("create timeseries root.sg1.d1.s1 with datatype=int32");
        userStatement.addBatch("create timeseries root.sg2.d1.s1 with datatype=int32");
        userStatement.executeBatch();
      } catch (Exception e) {
        assertEquals(
            System.lineSeparator()
                + "No permissions for this operation CREATE_TIMESERIES for SQL: \"create timeseries root.sg1.d1.s1 with datatype=int32\""
                + System.lineSeparator()
                + "No permissions for this operation CREATE_TIMESERIES for SQL: \"create timeseries root.sg2.d1.s1 with datatype=int32\""
                + System.lineSeparator(),
            e.getMessage());
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
