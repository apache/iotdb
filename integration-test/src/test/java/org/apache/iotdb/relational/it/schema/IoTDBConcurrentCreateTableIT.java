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

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(TableLocalStandaloneIT.class)
public class IoTDBConcurrentCreateTableIT {

  private static final String DATABASE_NAME = "concurrent_create_table_db";
  private static final int THREAD_COUNT = 16;
  private static final int TABLE_COUNT = 80_000;
  private static final int TABLES_PER_THREAD = TABLE_COUNT / THREAD_COUNT;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testConcurrentCreateEightyThousandTables() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
    }

    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    CountDownLatch ready = new CountDownLatch(THREAD_COUNT);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Void>> createTableTasks = new ArrayList<>(THREAD_COUNT);

    try {
      for (int threadIndex = 0; threadIndex < THREAD_COUNT; threadIndex++) {
        final int firstTableIndex = threadIndex * TABLES_PER_THREAD;
        createTableTasks.add(
            executor.submit(
                () -> {
                  boolean readySignaled = false;
                  try (Connection connection =
                          EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
                      Statement statement = connection.createStatement()) {
                    ready.countDown();
                    readySignaled = true;
                    assertTrue(
                        "Timed out waiting for all create-table threads to start",
                        start.await(1, TimeUnit.MINUTES));
                    for (int offset = 0; offset < TABLES_PER_THREAD; offset++) {
                      statement.execute(
                          "CREATE TABLE "
                              + DATABASE_NAME
                              + ".table_"
                              + (firstTableIndex + offset)
                              + " (tag STRING TAG, value INT32 FIELD)");
                    }
                  } finally {
                    if (!readySignaled) {
                      ready.countDown();
                    }
                  }
                  return null;
                }));
      }

      assertTrue("Timed out preparing create-table threads", ready.await(1, TimeUnit.MINUTES));
      start.countDown();
      executor.shutdown();
      for (Future<Void> createTableTask : createTableTasks) {
        createTableTask.get(2, TimeUnit.HOURS);
      }
    } finally {
      start.countDown();
      executor.shutdownNow();
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT count(*) FROM information_schema.tables WHERE database = '"
                    + DATABASE_NAME
                    + "'")) {
      assertTrue(resultSet.next());
      assertEquals(TABLE_COUNT, resultSet.getLong(1));
      assertFalse(resultSet.next());
    }
  }
}
