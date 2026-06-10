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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.RpcUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBAutoResizingBufferMemoryIT {

  private static final double AUTO_RESIZING_BUFFER_MEMORY_PROPORTION = 0.0003;
  private static final String CONFIG_FILE_ENTRY = "auto_resizing_buffer_memory_proportion=3.0E-4";
  private static final int DATANODE_MAX_HEAP_SIZE_IN_MB = 256;
  private static final int AUTO_RESIZING_BUFFER_COUNT_PER_CONNECTION = 2;
  private static final int CONNECTION_COUNT_OVERFLOW_MARGIN = 1;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoResizingBufferMemoryProportion(AUTO_RESIZING_BUFFER_MEMORY_PROPORTION);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeJVMConfig()
        .setMaxHeapSize(DATANODE_MAX_HEAP_SIZE_IN_MB);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAutoResizingBufferMemoryProportionConfigTakesEffect() throws Exception {
    Assert.assertTrue(
        EnvFactory.getEnv().getNodeWrapperList().stream()
            .allMatch(nodeWrapper -> checkConfigFileContains(nodeWrapper, CONFIG_FILE_ENTRY)));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.auto_resizing_buffer_memory");
      statement.execute(
          "CREATE TIMESERIES root.auto_resizing_buffer_memory.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      statement.execute(
          "INSERT INTO root.auto_resizing_buffer_memory.d1(time, s1) VALUES (1, 100)");

      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1 FROM root.auto_resizing_buffer_memory.d1")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(100, resultSet.getInt("root.auto_resizing_buffer_memory.d1.s1"));
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testNewConnectionsWithWritesAreRejectedWhenBufferMemoryIsExhausted()
      throws Exception {
    List<Connection> heldConnections = new ArrayList<>();
    boolean rejected = false;

    try {
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("CREATE DATABASE root.auto_resizing_buffer_reject");
        statement.execute(
            "CREATE TIMESERIES root.auto_resizing_buffer_reject.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");
      }

      int connectionCountToExhaustBufferMemory =
          calculateConnectionCountToExhaustAutoResizingBufferMemory();
      for (int i = 0; i < connectionCountToExhaustBufferMemory; i++) {
        try {
          Connection connection = EnvFactory.getEnv().getConnection();
          heldConnections.add(connection);
          try (Statement statement = connection.createStatement()) {
            statement.execute(
                String.format(
                    "INSERT INTO root.auto_resizing_buffer_reject.d1(time, s1) VALUES (%d, %d)",
                    i + 1, i));
          }
        } catch (Exception e) {
          rejected = true;
          break;
        }
      }
    } finally {
      for (Connection connection : heldConnections) {
        closeQuietly(connection);
      }
    }

    Assert.assertTrue(
        "Expected new connections with writes to be rejected after AutoResizingBuffer memory is exhausted",
        rejected);
  }

  private static void closeQuietly(Connection connection) {
    try {
      connection.close();
    } catch (SQLException ignored) {
      // ignored
    }
  }

  private static int calculateConnectionCountToExhaustAutoResizingBufferMemory() {
    long autoResizingBufferMemorySizeInBytes =
        (long)
            (DATANODE_MAX_HEAP_SIZE_IN_MB * 1024L * 1024L * AUTO_RESIZING_BUFFER_MEMORY_PROPORTION);
    int autoResizingBufferInitialSizePerConnection =
        AUTO_RESIZING_BUFFER_COUNT_PER_CONNECTION * RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY;
    return (int)
        (autoResizingBufferMemorySizeInBytes / autoResizingBufferInitialSizePerConnection
            + CONNECTION_COUNT_OVERFLOW_MARGIN);
  }

  private static boolean checkConfigFileContains(AbstractNodeWrapper nodeWrapper, String content) {
    try {
      String systemPropertiesPath =
          nodeWrapper.getNodePath()
              + File.separator
              + "conf"
              + File.separator
              + CommonConfig.SYSTEM_CONFIG_NAME;
      return new String(Files.readAllBytes(new File(systemPropertiesPath).toPath()))
          .contains(content);
    } catch (Exception ignore) {
      return false;
    }
  }
}
