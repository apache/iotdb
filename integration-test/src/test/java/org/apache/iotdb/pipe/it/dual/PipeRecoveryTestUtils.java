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

package org.apache.iotdb.pipe.it.dual;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBatchReqV2;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/** Assertions here never flush or write data while waiting for a Pipe to make progress. */
public final class PipeRecoveryTestUtils {
  private PipeRecoveryTestUtils() {}

  public static void configure(final BaseEnv env) {
    env.getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDatanodeMemoryProportion("3:3:1:1:3:0")
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false)
        .setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(1);
  }

  public static void execute(final BaseEnv env, final String... sqls) throws Exception {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      for (final String sql : sqls) {
        statement.execute(sql);
      }
    }
  }

  public static void dropDatabase(final BaseEnv env, final String database) throws Exception {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      final boolean exists;
      try (final ResultSet rows = statement.executeQuery("show databases " + database)) {
        exists = rows.next();
      }
      if (exists) {
        statement.execute("delete database " + database);
      }
    }
  }

  public static void createPipe(
      final BaseEnv sender,
      final String name,
      final String database,
      final String endpoint,
      final Map<String, String> sourceOverrides,
      final Map<String, String> processor,
      final Map<String, String> sinkOverrides)
      throws Exception {
    final Map<String, String> source = new HashMap<>();
    source.put("source.inclusion", "data.insert");
    source.put("source.path", database + ".**");
    // "log" aliases hybrid mode and can wait for a TsFile when a region first appears. These
    // cases require real-time rows without a flush, including a quiet sub-threshold batch.
    source.put("source.realtime.mode", "forced-log");
    source.put("user", SessionConfig.DEFAULT_USER);
    source.putAll(sourceOverrides);
    final Map<String, String> sink = new HashMap<>();
    sink.put("sink", "iotdb-thrift-sync-sink");
    sink.put("sink.node-urls", endpoint);
    sink.put("sink.parallel.tasks", "1");
    sink.put("sink.leader-cache.enable", "false");
    sink.put("sink.batch.enable", "false");
    sink.put("sink.format", "tablet");
    sink.putAll(sinkOverrides);
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) sender.getLeaderConfigNodeConnection()) {
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq(name, sink)
                      .setExtractorAttributes(source)
                      .setProcessorAttributes(processor))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe(name).getCode());
    }
  }

  public static TShowPipeInfo showPipe(final BaseEnv sender, final String name) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) sender.getLeaderConfigNodeConnection()) {
      final TShowPipeResp response =
          client.showPipe(
              new TShowPipeReq()
                  .setPipeName(name)
                  .setWhereClause(true)
                  .setUserName(SessionConfig.DEFAULT_USER));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      Assert.assertEquals(1, response.getPipeInfoListSize());
      return response.getPipeInfoList().get(0);
    }
  }

  public static void awaitDrained(final BaseEnv sender, final String name) {
    await()
        .pollInSameThread()
        .atMost(60, TimeUnit.SECONDS)
        .during(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final TShowPipeInfo pipe = showPipe(sender, name);
              Assert.assertEquals("RUNNING", pipe.getState());
              Assert.assertEquals(pipe.toString(), 0, remainingEventCount(sender, name));
            });
  }

  public static void awaitBacklog(final BaseEnv sender, final String name) {
    await()
        .pollInSameThread()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertTrue(
                  showPipe(sender, name).toString(), remainingEventCount(sender, name) > 0);
            });
  }

  private static long remainingEventCount(final BaseEnv sender, final String name)
      throws Exception {
    // ConfigNode returns -1 for a single-DataNode data-only pipe. SHOW PIPES resolves this
    // sentinel using that DataNode's live metrics, just as the public CLI does.
    try (final Connection connection = sender.getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet rows = statement.executeQuery("show pipe " + name)) {
      Assert.assertTrue("Missing pipe " + name, rows.next());
      final long count = Long.parseLong(rows.getString("RemainingEventCount"));
      Assert.assertTrue("Unknown remaining event count for " + name, count >= 0);
      Assert.assertFalse("Unexpected additional pipe", rows.next());
      return count;
    }
  }

  public static void assertRows(
      final BaseEnv env, final String sql, final String header, final String... rows)
      throws Exception {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet result = statement.executeQuery(sql)) {
      TestUtils.assertResultSetEqualUnordered(result, header, List.of(rows));
    }
  }

  public static void awaitRows(
      final BaseEnv env, final String sql, final String header, final String... rows) {
    await()
        .pollInSameThread()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(() -> assertRows(env, sql, header, rows));
  }

  public static void assertInt32Schema(final BaseEnv env, final String device) throws Exception {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet result = statement.executeQuery("show timeseries " + device + ".s1")) {
      Assert.assertTrue("Missing target schema for " + device, result.next());
      Assert.assertEquals(device + ".s1", result.getString("Timeseries"));
      Assert.assertEquals("INT32", result.getString("DataType"));
      Assert.assertFalse("Unexpected target schema for " + device, result.next());
    }
  }

  public static Set<String> batchDevices(final TPipeTransferReq request) throws Exception {
    final Set<String> devices = new HashSet<>();
    PipeTransferTabletBatchReqV2.fromTPipeTransferReq(request.deepCopy())
        .constructStatements()
        .forEach(
            statement ->
                statement.getDevicePaths().forEach(path -> devices.add(path.getFullPath())));
    return devices;
  }
}
