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

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBShowRegionIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTreeModelObjectSizeWithoutObjectFiles() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE DEVICE TEMPLATE t1 aligned ("
              + "s1 BOOLEAN, s2 TEXT, s3 INT32, s4 INT64, s5 FLOAT, s6 DOUBLE, "
              + "s7 STRING, s8 DATE, s9 BLOB, s10 TIMESTAMP)");
      statement.execute("CREATE DATABASE root.g1");
      statement.execute("SET DEVICE TEMPLATE t1 TO root.g1.d1");
      statement.execute("CREATE TIMESERIES USING DEVICE TEMPLATE ON root.g1.d1");
      statement.execute(
          "INSERT INTO root.g1.d1(timestamp, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10) "
              + "VALUES (1, true, 'text', 2026, 1234567890123, 3.1415, 3.1415926535, "
              + "'string', '2026-03-01', X'68656C6C6F', 1740816000000)");

      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollDelay(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                boolean hasDataRegion = false;
                try (final ResultSet resultSet =
                    statement.executeQuery("SHOW REGIONS OF DATABASE root.g1")) {
                  while (resultSet.next()) {
                    if (!"DataRegion".equals(resultSet.getString(ColumnHeaderConstant.TYPE))) {
                      continue;
                    }
                    hasDataRegion = true;
                    Assert.assertEquals(
                        "0 B", resultSet.getString(ColumnHeaderConstant.OBJECT_SIZE));
                  }
                }
                Assert.assertTrue(hasDataRegion);
              });
    }
  }
}
