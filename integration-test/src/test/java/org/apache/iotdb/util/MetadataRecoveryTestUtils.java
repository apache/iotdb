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

package org.apache.iotdb.util;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.generatePatternTreeBuffer;
import static org.junit.Assert.assertEquals;

/** Helpers shared by metadata recovery ITs; all clients stay on the requested DataNode. */
public final class MetadataRecoveryTestUtils {
  public static final String TIMESERIES_HEADER =
      "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,";

  private MetadataRecoveryTestUtils() {}

  public static Connection connection(DataNodeWrapper node, String dialect) throws SQLException {
    return EnvFactory.getEnv()
        .getConnection(node, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, dialect);
  }

  public static void execute(Connection connection, String... sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(90);
      for (String command : sql) {
        statement.execute(command);
      }
    }
  }

  public static void assertRows(Connection connection, String sql, String header, String... rows)
      throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(sql)) {
      TestUtils.assertResultSetEqualUnordered(result, header, Arrays.asList(rows));
    }
  }

  public static void property(Object config, String key, String value) {
    Properties properties = new Properties();
    properties.setProperty(key, value);
    ((MppBaseConfig) config).updateProperties(properties);
  }

  public static int schemaRegion(String database, String path) throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final var result =
          client.getSchemaPartitionTable(
              new TSchemaPartitionReq(generatePatternTreeBuffer(new String[] {path})));
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.getStatus().getCode());
      final var slots = result.getSchemaPartitionTable().get(database);
      assertEquals("Expected exactly one partition for " + path, 1, slots.size());
      return slots.values().iterator().next().getId();
    }
  }

  /** A file barrier in one child JVM; closing it releases workers even after an assertion fails. */
  public static final class Gate implements AutoCloseable {
    private final Path pause;
    private final Path watch;
    private final Path reached;

    public Gate(AbstractNodeWrapper node, Enum<?> point, boolean block) throws IOException {
      Path directory = Paths.get(node.getNodePath(), "killpoints");
      Files.createDirectories(directory);
      String name = KillPoint.enumToString(point);
      pause = directory.resolve(name + ".pause");
      watch = directory.resolve(name + ".watch");
      reached = directory.resolve(name + ".reached");
      Files.deleteIfExists(reached);
      Files.write(block ? pause : watch, new byte[0]);
    }

    public void await() {
      Awaitility.await()
          .pollInterval(20, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> Files.exists(reached));
    }

    public void release() throws IOException {
      Files.deleteIfExists(pause);
    }

    @Override
    public void close() throws IOException {
      release();
      Files.deleteIfExists(watch);
      Files.deleteIfExists(reached);
    }
  }
}
