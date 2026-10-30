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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBPipeProgressIndexCleanupIT {

  private static final String DATABASE = "root.pipe_progress_index";
  private static final String CLEANUP_LOG =
      "Cleared pipe progress indexes of the deleted data region";

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDropDatabaseClearsPipeProgressIndexes() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("CREATE TIMESERIES " + DATABASE + ".d1.s1 INT32");
      statement.execute("INSERT INTO " + DATABASE + ".d1(timestamp, s1) VALUES (1, 1)");
      statement.execute("FLUSH");
      statement.execute("DROP DATABASE " + DATABASE);
    }

    final File logFile =
        new File(
            EnvFactory.getEnv().getDataNodeWrapper(0).getNodePath() + File.separator + "logs",
            "log_datanode_all.log");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertTrue(logFile.getPath(), logFile.isFile());
              Assert.assertTrue(
                  CLEANUP_LOG,
                  Files.readAllLines(logFile.toPath(), StandardCharsets.UTF_8).stream()
                      .anyMatch(line -> line.contains(CLEANUP_LOG)));
            });
  }
}
