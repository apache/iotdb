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
package org.apache.iotdb.db.it.last;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.assertResultSetEqual;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLastQueryVirtualRegionIT {

  private static final String[] LAST_HEADER = {"Time", "Timeseries", "Value", "DataType"};

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableLastCache(true);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNonAlignedDeviceWithoutDataPartition() throws Exception {
    testDeviceWithoutDataPartition(false);
  }

  @Test
  public void testAlignedDeviceWithoutDataPartition() throws Exception {
    testDeviceWithoutDataPartition(true);
  }

  private void testDeviceWithoutDataPartition(boolean aligned) throws Exception {
    String prefix = aligned ? "root.last_virtual_aligned" : "root.last_virtual_normal";
    String populatedDatabase = prefix + "_data";
    String emptyDatabase = prefix + "_empty";
    String populatedDevice = populatedDatabase + ".d";
    String emptyDevice = emptyDatabase + ".d";
    // A separate database guarantees that the empty device has no data partition, even if
    // its series partition slot happens to collide with that of the populated device.
    try (Connection connection =
            EnvFactory.getEnv()
                .getConnectionWithSpecifiedDataNode(EnvFactory.getEnv().getDataNodeWrapper(0));
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + populatedDatabase);
      statement.execute("create database " + emptyDatabase);
      statement.execute("create timeseries " + populatedDevice + ".s1 INT32");
      statement.execute("insert into " + populatedDevice + "(time,s1) values(1,11)");
      if (aligned) {
        statement.execute("create aligned timeseries " + emptyDevice + "(s1 INT32,s2 INT32)");
      } else {
        statement.execute("create timeseries " + emptyDevice + ".s1 INT32");
        statement.execute("create timeseries " + emptyDevice + ".s2 INT32");
      }

      String mixedLastQuery =
          "select last * from "
              + populatedDatabase
              + ".**, "
              + emptyDatabase
              + ".** order by timeseries";
      String[] populatedLast = {"1," + populatedDevice + ".s1,11,INT32,"};
      String[] emptyDeviceHeader = {"Time", emptyDevice + ".s1", emptyDevice + ".s2"};
      for (int i = 0; i < 2; i++) {
        statement.execute("clear schema cache on cluster");
        // Wildcard schema fetching leaves the device schema cache cold. The mixed LAST query
        // must still execute the populated branch and the empty virtual-region branch.
        assertQuery(statement, mixedLastQuery, LAST_HEADER, populatedLast);
        assertQuery(
            statement, "select s1,s2 from " + emptyDevice, emptyDeviceHeader, new String[0]);
        assertQuery(statement, "select last s1,s2 from " + emptyDevice, LAST_HEADER, new String[0]);
        // Repeat with the schema cache warmed by the exact query.
        assertQuery(statement, mixedLastQuery, LAST_HEADER, populatedLast);
        assertQuery(
            statement, "select s1,s2 from " + emptyDevice, emptyDeviceHeader, new String[0]);
      }

      statement.execute(
          "insert into "
              + emptyDevice
              + "(time,s1,s2)"
              + (aligned ? " aligned" : "")
              + " values(2,21,22)");
      assertQuery(
          statement,
          "select last s1,s2 from " + emptyDevice + " order by timeseries",
          LAST_HEADER,
          new String[] {
            "2," + emptyDevice + ".s1,21,INT32,", "2," + emptyDevice + ".s2,22,INT32,"
          });
      assertQuery(
          statement,
          "select s1,s2 from " + emptyDevice,
          emptyDeviceHeader,
          new String[] {"2,21,22,"});
    }
  }

  private void assertQuery(Statement statement, String sql, String[] header, String[] rows)
      throws Exception {
    try (ResultSet resultSet = statement.executeQuery(sql)) {
      assertResultSetEqual(resultSet, String.join(",", header) + ",", rows);
    }
  }
}
