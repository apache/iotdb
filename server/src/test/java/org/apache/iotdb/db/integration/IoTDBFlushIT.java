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

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IoTDBFlushIT {

  private static int partitionInterval = 100;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    StorageEngine.setEnablePartition(true);
    StorageEngine.setTimePartitionInterval(partitionInterval);
  }

  @After
  public void tearDown() throws Exception {
    StorageEngine.setEnablePartition(false);
    StorageEngine.setTimePartitionInterval(-1);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSetEncodingRegularSimpleTimeseriesFlush() {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1000)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,2200)");
      statement.execute("flush");

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.db_0.tab0")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetEncodingRegularMultiTimeseriesFlush() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1000)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,2200)");
      statement.execute("create timeseries root.db_0.tab0.two with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,two) values(1,200)");
      statement.execute("insert into root.db_0.tab0(time ,two) values(2,300)");
      statement.execute("flush");

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.db_0.tab0")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(2, count);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetEncodingRegularMultiTimeseriesFlushAndInsertRegularData() {
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      statement.execute("create timeseries root.db_0.tab0.salary with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1000)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(4,2200)");
      statement.execute("create timeseries root.db_0.tab0.two with datatype=INT64,encoding=REGULAR");
      statement.execute("insert into root.db_0.tab0(time ,two) values(1,200)");
      statement.execute("insert into root.db_0.tab0(time ,two) values(2,300)");
      statement.execute("flush");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(1,1200)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(2,1100)");
      statement.execute("insert into root.db_0.tab0(time ,salary) values(3,1000)");

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.db_0.tab0.salary")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(3, count);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
