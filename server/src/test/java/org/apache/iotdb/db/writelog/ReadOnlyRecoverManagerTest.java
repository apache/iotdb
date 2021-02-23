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
package org.apache.iotdb.db.writelog;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.writelog.manager.ReadOnlyRecoverManager;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ReadOnlyRecoverManagerTest {

  private static int partitionInterval = 100;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    insertData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setReadOnly(false);
  }

  @Test
  public void testRecoverExecute() {
    IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    ReadOnlyRecoverManager.getInstance().executeRecover();
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertEquals(false, IoTDBDescriptor.getInstance().getConfig().isReadOnly());
  }

  @Test
  public void testRetryDefaultSleepIntervalAndAttempts() {
    assertEquals(3, IoTDBDescriptor.getInstance().getConfig().getReadOnlyRecoverRetryAttempts());
    assertEquals(
        10 * 60 * 1000L,
        IoTDBDescriptor.getInstance().getConfig().getReadOnlyRecoverRetrySleepInterval());
  }

  private static void insertData() throws ClassNotFoundException {
    List<String> sqls =
        new ArrayList<>(
            Arrays.asList(
                "SET STORAGE GROUP TO root.test1",
                "SET STORAGE GROUP TO root.test2",
                "CREATE TIMESERIES root.test1.s0 WITH DATATYPE=INT64,ENCODING=PLAIN",
                "CREATE TIMESERIES root.test2.s0 WITH DATATYPE=INT64,ENCODING=PLAIN"));
    // 10 partitions, each one with one seq file and one unseq file
    for (int i = 0; i < 10; i++) {
      // seq files
      for (int j = 1; j <= 2; j++) {
        sqls.add(
            String.format(
                "INSERT INTO root.test%d(timestamp, s0) VALUES (%d, %d)",
                j, i * partitionInterval + 50, i * partitionInterval + 50));
      }
      // last file is unclosed
      if (i < 9) {
        sqls.add("FLUSH");
      }
      // unseq files
      for (int j = 1; j <= 2; j++) {
        sqls.add(
            String.format(
                "INSERT INTO root.test%d(timestamp, s0) VALUES (%d, %d)",
                j, i * partitionInterval, i * partitionInterval));
      }
      sqls.add("MERGE");
      // last file is unclosed
      if (i < 9) {
        sqls.add("FLUSH");
      }
    }
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
