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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBLoadConfigurationTableIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void loadConfiguration() throws IOException {
    DataNodeWrapper dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
    String confPath =
        dataNodeWrapper.getNodePath()
            + File.separator
            + "conf"
            + File.separator
            + "iotdb-system.properties";
    long length = new File(confPath).length();
    try (FileWriter fileWriter = new FileWriter(confPath, true)) {
      fileWriter.write(System.lineSeparator());
      fileWriter.write("target_compaction_file_size=t");
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("LOAD CONFIGURATION");
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("NumberFormatException"));
    } finally {
      try (FileChannel fileChannel =
          FileChannel.open(new File(confPath).toPath(), StandardOpenOption.WRITE)) {
        fileChannel.truncate(length);
      }
    }
  }

  // `show configuration` must display the in-memory effective value, not the raw config-file
  // value. Several hot-reloaded parameters have setters that rewrite the loaded value (e.g.
  // `if (x > 0) setX(x)` skips non-positive values; loadFixedSizeLimitForQuery rewrites `<=0`
  // to a computed default). This test verifies those keys show the effective value after
  // `load configuration`.
  @Test
  public void showConfigurationDisplaysEffectiveValue() throws Exception {
    DataNodeWrapper dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
    String confPath =
        dataNodeWrapper.getNodePath()
            + File.separator
            + "conf"
            + File.separator
            + "iotdb-system.properties";

    // Case 1: non-positive guard params (`if (x > 0) setX(x)`) must display the effective
    // default value, not the raw file value (0 / -1).
    Map<String, String> afterGuardReload =
        appendLinesLoadAndShow(
            confPath,
            "cte_buffer_size_in_bytes=0",
            "max_rows_in_cte_buffer=-1",
            "max_sub_task_num_for_information_table_scan=0");
    Assert.assertEquals("131072", afterGuardReload.get("cte_buffer_size_in_bytes"));
    Assert.assertEquals("1000", afterGuardReload.get("max_rows_in_cte_buffer"));
    Assert.assertEquals("4", afterGuardReload.get("max_sub_task_num_for_information_table_scan"));

    // Case 2: a valid value is not rewritten, so display must equal the file value (regression).
    Map<String, String> afterValidReload =
        appendLinesLoadAndShow(confPath, "cte_buffer_size_in_bytes=262144");
    Assert.assertEquals("262144", afterValidReload.get("cte_buffer_size_in_bytes"));

    // Case 3: params whose template default is 0 are always rewritten to a computed default,
    // so they must never display 0 (this was always wrong, even without a bad file value).
    Map<String, String> defaultsReload = appendLinesLoadAndShow(confPath);
    assertPositiveNonZero(defaultsReload, "sort_buffer_size_in_bytes");
    assertPositiveNonZero(defaultsReload, "mods_cache_size_limit_per_fi_in_bytes");
  }

  // Appends the given lines to the config file, runs `load configuration`, fetches the
  // `show configuration` result, and restores the file to its original length.
  private Map<String, String> appendLinesLoadAndShow(String confPath, String... lines)
      throws Exception {
    long length = new File(confPath).length();
    try {
      if (lines.length > 0) {
        try (FileWriter fileWriter = new FileWriter(confPath, true)) {
          fileWriter.write(System.lineSeparator());
          for (String line : lines) {
            fileWriter.write(line);
            fileWriter.write(System.lineSeparator());
          }
        }
      }
      try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("LOAD CONFIGURATION");
      }
      return fetchShowConfiguration();
    } finally {
      try (FileChannel fileChannel =
          FileChannel.open(new File(confPath).toPath(), StandardOpenOption.WRITE)) {
        fileChannel.truncate(length);
      }
    }
  }

  private Map<String, String> fetchShowConfiguration() throws Exception {
    Map<String, String> result = new HashMap<>();
    try (ITableSession tableSessionConnection = EnvFactory.getEnv().getTableSessionConnection()) {
      SessionDataSet sessionDataSet =
          tableSessionConnection.executeQueryStatement("show configuration");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        String name = iterator.getString(1);
        String value = iterator.isNull(2) ? null : iterator.getString(2);
        result.put(name, value);
      }
    }
    return result;
  }

  private static void assertPositiveNonZero(Map<String, String> configMap, String key) {
    String value = configMap.get(key);
    Assert.assertNotNull("show configuration is missing key: " + key, value);
    long parsed = Long.parseLong(value);
    Assert.assertTrue(
        key + " should display a positive effective value, but was " + value, parsed > 0);
  }
}
