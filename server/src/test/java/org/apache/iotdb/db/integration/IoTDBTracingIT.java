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

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBTracingIT {

  private static String[] sqls =
      new String[] {
        "set storage group to root.ln",
        "create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)",
        "insert into root.ln.wf01.wt01(timestamp,status) values(1509465660000,true)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void tracingTest() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      Assert.assertEquals(false, config.isEnablePerformanceTracing());

      statement.execute("tracing on");
      Assert.assertEquals(true, config.isEnablePerformanceTracing());

      statement.execute("tracing off");
      Assert.assertEquals(false, config.isEnablePerformanceTracing());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void tracingClearAllTest() throws ClassNotFoundException {
    System.out.println("do tracingClearAllTest");
    importData();
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String selectSql = "SELECT * FROM root.ln.wf01.wt01";

      statement.execute(selectSql);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          System.out.println(resultSet.getString(1) + "  " + resultSet.getString(2));
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
      File tracingClearBeforeTestFile =
          SystemFileFactory.INSTANCE.getFile(
              IoTDBDescriptor.getInstance().getConfig().getTracingDir()
                  + File.separator
                  + IoTDBConstant.TRACING_LOG);
      Assert.assertTrue(tracingClearStatues(tracingClearBeforeTestFile));

      String clearSql = "tracing clear all";
      statement.execute(clearSql);
      File tracingClearAfterTestFile =
          SystemFileFactory.INSTANCE.getFile(
              IoTDBDescriptor.getInstance().getConfig().getTracingDir()
                  + File.separator
                  + IoTDBConstant.TRACING_LOG);
      Assert.assertFalse(tracingClearStatues(tracingClearAfterTestFile));
      statement.execute("tracing off");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void importData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      statement.execute("tracing on");
      Assert.assertEquals(true, config.isEnablePerformanceTracing());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public boolean tracingClearStatues(File tracingClearAllTestFile) throws IOException {
    BufferedReader tracingClearAllTestBufferedReader =
        new BufferedReader(new FileReader(tracingClearAllTestFile));
    try {
      String str;
      while ((str = tracingClearAllTestBufferedReader.readLine()) != null) {
        if (str.contains("Query Id")) {
          return true;
        } else {
          return false;
        }
      }
      return false;
    } catch (FileNotFoundException e) {
      return false;
    } finally {
      tracingClearAllTestBufferedReader.close();
    }
  }
}
