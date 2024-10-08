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

package org.apache.iotdb.db.it.schema.regionscan;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.schema.regionscan.IoTDBActiveRegionScanIT.COUNT_DEVICES_COLUMN_NAMES;
import static org.apache.iotdb.db.it.schema.regionscan.IoTDBActiveRegionScanIT.COUNT_TIMESERIES_COLUMN_NAMES;
import static org.apache.iotdb.db.it.schema.regionscan.IoTDBActiveRegionScanIT.SHOW_DEVICES_COLUMN_NAMES;
import static org.apache.iotdb.db.it.schema.regionscan.IoTDBActiveRegionScanIT.SHOW_TIMESERIES_COLUMN_NAMES;
import static org.apache.iotdb.db.it.schema.regionscan.IoTDBActiveRegionScanIT.basicCountActiveDeviceTest;
import static org.apache.iotdb.db.it.schema.regionscan.IoTDBActiveRegionScanIT.basicShowActiveDeviceTest;
import static org.apache.iotdb.db.it.schema.regionscan.IoTDBActiveRegionScanIT.common_insert_sqls;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBActiveRegionScanWithModsIT extends AbstractSchemaIT {

  private static String[] delete_sqls =
      new String[] {
        "delete from root.sg.aligned.** where time >= 1 and time <= 3",
        "delete from root.sg.aligned.d1.* where time = 20",
        "delete from root.sg.aligned.d2.* where time = 30",
        "delete from root.** where time >=9 and time <=15",
        "delete from root.sg.unaligned.** where time >= 20 and time <= 30",
        "delete from root.sg.aligned.** where time > 40 and time <= 43",
        "delete from root.sg.unaligned.d2.* where time > 40 and time <= 43"
      };

  public IoTDBActiveRegionScanWithModsIT(final SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  public static void insertData() {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      // create aligned and non-aligned time series
      for (final String sql : common_insert_sqls) {
        statement.addBatch(sql);
      }
      for (final String sql : delete_sqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (final Exception e) {
      fail(e.getMessage());
    }
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    tearDownEnvironment();
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void showActiveDataWithMods() {
    String sql = "show devices where time < 5";
    String[] retArray = new String[] {"root.sg.unaligned.d2"};
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    sql = "count devices where time < 5";
    long value = 1;
    basicCountActiveDeviceTest(sql, COUNT_DEVICES_COLUMN_NAMES, value);

    sql = "show timeseries where time < 5";
    retArray = new String[] {"root.sg.unaligned.d2.s1"};
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);

    sql = "count timeseries where time < 5";
    value = 1;
    basicCountActiveDeviceTest(sql, COUNT_TIMESERIES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveDataWithMods2() {
    String sql = "show devices where time < 31 and time > 15";
    String[] retArray = new String[] {"root.sg.aligned.d1"};
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    sql = "count devices where time < 31 and time > 15";
    long value = 1;
    basicCountActiveDeviceTest(sql, COUNT_DEVICES_COLUMN_NAMES, value);

    sql = "show timeseries where time < 31 and time > 15";
    retArray =
        new String[] {
          "root.sg.aligned.d1.s1", "root.sg.aligned.d1.s2",
        };
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);

    sql = "count timeseries where time < 31 and time > 15";
    value = 2;
    basicCountActiveDeviceTest(sql, COUNT_TIMESERIES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveDataWithMods3() {
    String sql = "show devices where time > 7 and time < 20";
    String[] retArray =
        new String[] {"root.sg.aligned.d1", "root.sg.aligned.d2", "root.sg.unaligned.d2"};
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    sql = "count devices where time > 7 and time < 20";
    long value = 3;
    basicCountActiveDeviceTest(sql, COUNT_DEVICES_COLUMN_NAMES, value);

    sql = "show timeseries where time > 7 and time < 20";
    retArray =
        new String[] {"root.sg.aligned.d1.s2", "root.sg.aligned.d2.s4", "root.sg.unaligned.d2.s1"};
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);

    sql = "count timeseries where time > 7 and time < 20";
    value = 3;
    basicCountActiveDeviceTest(sql, COUNT_TIMESERIES_COLUMN_NAMES, value);
  }

  @Test
  public void showActiveDataWithMods4() {
    String sql = "show devices where time > 40";
    String[] retArray =
        new String[] {
          "root.sg.aligned.d1",
          "root.sg.aligned.d2",
          "root.sg.unaligned.d2",
          "root.sg.unaligned.d3",
        };
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    sql = "count devices where time > 40";
    long value = 4;
    basicCountActiveDeviceTest(sql, COUNT_DEVICES_COLUMN_NAMES, value);

    sql = "show timeseries where time > 40";
    retArray =
        new String[] {
          "root.sg.aligned.d1.s1",
          "root.sg.aligned.d1.s2",
          "root.sg.aligned.d2.s3",
          "root.sg.aligned.d2.s4",
          "root.sg.unaligned.d2.s1",
          "root.sg.unaligned.d3.s4",
        };
    basicShowActiveDeviceTest(sql, SHOW_TIMESERIES_COLUMN_NAMES, retArray);

    sql = "count timeseries where time > 40";
    value = 6;
    basicCountActiveDeviceTest(sql, COUNT_TIMESERIES_COLUMN_NAMES, value);
  }
}
