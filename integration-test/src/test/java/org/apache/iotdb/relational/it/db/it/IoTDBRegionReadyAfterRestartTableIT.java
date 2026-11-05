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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Verifies that restartCluster() waits for the Regions of a table-model-only cluster. */
@RunWith(IoTDBTestRunner.class)
@Category(TableClusterIT.class)
public class IoTDBRegionReadyAfterRestartTableIT {

  private static final String DATABASE = "restart_ready";

  @BeforeClass
  public static void setUpClass() {
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testWriteAfterClusterRestart() throws SQLException {
    try (final Connection connection = EnvFactory.getEnv().getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + DATABASE);
      statement.execute("use \"" + DATABASE + "\"");
      statement.execute("create table t (tag string tag, s int32 field)");
      statement.execute("insert into t (tag, time, s) values ('d1', 1, 1)");
    }

    TestUtils.restartCluster(EnvFactory.getEnv());

    try (final Connection connection = EnvFactory.getEnv().getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("use \"" + DATABASE + "\"");
      // restartCluster() returns only after the Regions are Running; a write before that is
      // rejected with NO_AVAILABLE_REGION_GROUP.
      try (final ResultSet regions = statement.executeQuery("show regions")) {
        int regionCount = 0;
        while (regions.next()) {
          Assert.assertEquals(
              RegionStatus.Running.getStatus(), regions.getString(ColumnHeaderConstant.STATUS));
          regionCount++;
        }
        Assert.assertTrue("no table-model Region is reported", regionCount > 0);
      }
      statement.execute("insert into t (tag, time, s) values ('d1', 2, 2)");
      try (final ResultSet rows = statement.executeQuery("select s from t order by time")) {
        Assert.assertTrue(rows.next());
        Assert.assertEquals(1, rows.getInt(1));
        Assert.assertTrue(rows.next());
        Assert.assertEquals(2, rows.getInt(1));
        Assert.assertFalse(rows.next());
      }
    }
  }
}
