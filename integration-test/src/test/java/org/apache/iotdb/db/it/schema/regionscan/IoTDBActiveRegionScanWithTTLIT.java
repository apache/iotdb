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

import static org.apache.iotdb.db.it.schema.regionscan.IoTDBActiveRegionScanIT.SHOW_DEVICES_COLUMN_NAMES;
import static org.apache.iotdb.db.it.schema.regionscan.IoTDBActiveRegionScanIT.basicShowActiveDeviceTest;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBActiveRegionScanWithTTLIT extends AbstractSchemaIT {

  public IoTDBActiveRegionScanWithTTLIT(final SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  private static String[] sqls =
      new String[] {
        "create timeseries root.sg.d1.s1 WITH DATATYPE=INT64, encoding=RLE",
        "create timeseries root.sg.d1.s2 WITH DATATYPE=INT32, encoding=Gorilla",
        "create timeseries root.sg.d2.s1 WITH DATATYPE=INT64, encoding=RLE",
        "create timeseries root.sg.d2.s2 WITH DATATYPE=INT32, encoding=Gorilla",
        "insert into root.sg.d1(time, s1, s2) values(1, 1, 2)",
        "insert into root.sg.d1(time, s1, s2) values(2, 2, 3)",
        "insert into root.sg.d1(time, s1, s2) values(3, 3, 4)",
        "insert into root.sg.d1(time, s1, s2) values(5, 5, 6)",
        "insert into root.sg.d1(time, s1, s2) values(6, 6, 7)",
        "insert into root.sg.d1(time, s1, s2) values(7, 7, 8)",
        "insert into root.sg.d1(time, s1, s2) values(8, null, 9)",
        "insert into root.sg.d1(time, s1, s2) values(9, 9, 10)",
        "insert into root.sg.d1(time, s1, s2) values(10, 10, 11)",
        "flush",
        "insert into root.sg.d2(time, s1, s2) values(now(), null, 9)",
        "insert into root.sg.d2(time, s1, s2) values(now(), 9, 10)",
        "insert into root.sg.d2(time, s1, s2) values(now(), 10, 11)"
      };

  public static void insertData() {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      for (final String sql : sqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (final Exception e) {
      fail(e.getMessage());
    }
  }

  private static void setTTL() {
    final String[] ttl_sqls = {"set ttl to root.sg.d1 3600000", "set ttl to root.sg.d2 3600000"};
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      for (final String sql : ttl_sqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (final Exception e) {
      fail(e.getMessage());
    }
  }

  private static void unsetTTL() {
    final String[] ttl_sqls = {"unset ttl to root.sg.d1", "unset ttl to root.sg.d2"};
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      for (final String sql : ttl_sqls) {
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
    final String sql = "show devices where time > 0";
    String[] retArray = new String[] {"root.sg.d1", "root.sg.d2"};
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    setTTL();

    retArray = new String[] {"root.sg.d2"};
    basicShowActiveDeviceTest(sql, SHOW_DEVICES_COLUMN_NAMES, retArray);

    unsetTTL();
  }
}
