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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBCreateSnapshotIT {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void createSnapshotTest() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // test before creating snapshot
      checkShowTimeseries(statement);

      // create snapshot
      statement.execute("CREATE SNAPSHOT FOR SCHEMA");
      File snapshotFile = new File(config.getSchemaDir() + File.separator + "mtree-1.snapshot");

      // test snapshot file exists
      Assert.assertTrue(snapshotFile.exists());

      // test snapshot content correct
      Set<String> e1 = new HashSet<>(Arrays.asList("2,s0,,1,2,1,,-1,0", "2,s1,,2,2,1,,-1,0",
          "2,s2,,3,2,1,,-1,0", "2,s3,,5,0,1,,-1,0", "2,s4,,0,0,1,,-1,0"));
      Set<String> e2 = new HashSet<>(Arrays.asList("2,s0,,1,2,1,,-1,0", "2,s1,,5,0,1,,-1,0",
          "2,s2,,0,0,1,,-1,0"));

      try (BufferedReader br = new BufferedReader(new FileReader(snapshotFile))) {
        for (int i = 0; i < 5; ++i) {
          String actual = br.readLine();
          Assert.assertTrue(e1.removeIf(candidate -> candidate.equals(actual)));
        }
        Assert.assertTrue(e1.isEmpty());

        Assert.assertEquals("1,d0,9223372036854775807,5", br.readLine());

        for (int i = 0; i < 3; ++i) {
          String actual = br.readLine();
          Assert.assertTrue(e2.removeIf(candidate -> candidate.equals(actual)));
        }
        Assert.assertTrue(e2.isEmpty());

        Assert.assertEquals("1,d1,9223372036854775807,3", br.readLine());
        Assert.assertEquals("0,vehicle,2", br.readLine());
        Assert.assertEquals("0,root,1", br.readLine());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // test restart
    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {
      checkShowTimeseries(statement);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData() throws SQLException {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {

      String[] creationSqls = new String[]{
          "SET STORAGE GROUP TO root.vehicle.d0",
          "SET STORAGE GROUP TO root.vehicle.d1",
          "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
          "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
          "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
          "CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN"
      };

      for (String sql : creationSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void checkShowTimeseries(Statement statement) throws SQLException {
    boolean hasResultSet = statement.execute("SHOW TIMESERIES");
    assertTrue(hasResultSet);

    try (ResultSet resultSet = statement.getResultSet()) {
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(8, cnt);
    }
  }
}
