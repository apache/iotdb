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

package org.apache.iotdb.pipe.it.single;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;

/**
 * Verifies that the thrift pipe receiver loads asynchronously TsFiles with the privileges of the
 * authenticated sender. The asynchronous load is executed by the active loader in another thread,
 * so the receiver must hand the sender's identity over to it explicitly.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBPipeReceiverAsyncLoadAuthIT {

  private static final String DATABASE = "root.pipe_receiver_async_auth";
  private static final String DEVICE = DATABASE + ".d1";
  private static final String NO_WRITE_MEASUREMENT = "no_write";
  private static final String WRITE_MEASUREMENT = "write";
  private static final IMeasurementSchema NO_WRITE_SCHEMA =
      new MeasurementSchema(NO_WRITE_MEASUREMENT, TSDataType.INT32, TSEncoding.RLE);
  private static final IMeasurementSchema WRITE_SCHEMA =
      new MeasurementSchema(WRITE_MEASUREMENT, TSDataType.INT32, TSEncoding.RLE);

  private static final String NO_WRITE_USER = "pipe_receiver_no_write_user";
  private static final String WRITE_USER = "pipe_receiver_write_user";
  private static final String PASSWORD = "StrngPsWd@623451";
  private static final String SENDER_CLUSTER_ID = "IT_PIPE_RECEIVER_SENDER_CLUSTER";
  private static final long TIME_PARTITION_INTERVAL = 10 * 1000L;
  private static final int POINT_COUNT = 10;

  private static File tmpDir;

  @BeforeClass
  public static void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("pipe-receiver-async-auth").toUri());
    // Match the configuration of the other pipe integration tests: the receiver refuses handshakes
    // when the DataNode cannot reserve pipe memory.
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false)
        .setEnforceStrongPassword(false)
        .setTimePartitionInterval(TIME_PARTITION_INTERVAL);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setLoadActiveListeningCheckIntervalSeconds(1);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    FileUtils.deleteDirectory(tmpDir);
  }

  @Test
  public void testAsyncLoadStrategyDoesNotElevatePipeSender() throws Exception {
    prepareSchema();
    createUser(NO_WRITE_USER, PASSWORD);
    createUser(WRITE_USER, PASSWORD);
    grantUserSeriesPrivilege(WRITE_USER, PrivilegeType.WRITE_DATA, DATABASE + ".**");

    final File noWriteTsFile = new File(tmpDir, "1-0-0-0.tsfile");
    final File writeTsFile = new File(tmpDir, "2-0-0-0.tsfile");
    generateTsFile(noWriteTsFile, NO_WRITE_SCHEMA);
    generateTsFile(writeTsFile, WRITE_SCHEMA);

    // Control: the synchronous strategy loads with the privileges of the sender, so the account
    // without grants is rejected here.
    assertSealRejected(noWriteTsFile, NO_WRITE_USER, false);

    // The asynchronous strategy must not turn the same account into the super user.
    assertSealAccepted(noWriteTsFile, NO_WRITE_USER, true);

    // Positive control: the same asynchronous flow succeeds for an account holding WRITE_DATA.
    assertSealAccepted(writeTsFile, WRITE_USER, true);

    waitUntilActiveLoadIsIdle(TimeUnit.SECONDS.toMillis(60));
    assertCount(NO_WRITE_MEASUREMENT, 0);
    assertCount(WRITE_MEASUREMENT, POINT_COUNT);
  }

  private static void prepareSchema() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + DATABASE);
      statement.execute(
          String.format(
              "create timeseries %s.%s %s",
              DEVICE, NO_WRITE_MEASUREMENT, NO_WRITE_SCHEMA.getType()));
      statement.execute(
          String.format(
              "create timeseries %s.%s %s", DEVICE, WRITE_MEASUREMENT, WRITE_SCHEMA.getType()));
    }
  }

  private static void generateTsFile(final File tsFile, final IMeasurementSchema schema)
      throws Exception {
    try (final TsFileGenerator generator = new TsFileGenerator(tsFile)) {
      generator.registerTimeseries(DEVICE, Collections.singletonList(schema));
      generator.generateData(DEVICE, POINT_COUNT, TIME_PARTITION_INTERVAL / 10, false);
    }
  }

  /**
   * Sends a TsFile to the DataNode pipe receiver with the given load strategy and returns the seal
   * status. The handshake carries a cluster id different from the receiver's, so the receiver
   * treats the sender as a remote cluster.
   */
  private static TPipeTransferResp sendTsFile(
      final File tsFile, final String user, final boolean asyncLoadStrategy) throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    try (final IoTDBSyncClient client =
        new IoTDBSyncClient(
            new ThriftClientProperty.Builder().build(),
            dataNode.getIp(),
            dataNode.getPort(),
            false,
            null,
            null)) {
      final TSOpenSessionResp openSessionResp = client.openSession(buildOpenSessionReq(user));
      Assert.assertEquals(
          "Failed to open the session of " + user + ": " + openSessionResp.getStatus(),
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          openSessionResp.getStatus().getCode());

      try {
        final TPipeTransferResp handshakeResp =
            client.pipeTransfer(buildHandshakeReq(user, asyncLoadStrategy));
        Assert.assertEquals(
            "Handshake failed: " + handshakeResp.getStatus(),
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            handshakeResp.getStatus().getCode());

        final String fileName = tsFile.getName();
        final TPipeTransferResp pieceResp =
            client.pipeTransfer(
                PipeTransferTsFilePieceReq.toTPipeTransferReq(
                    fileName, 0L, Files.readAllBytes(tsFile.toPath())));
        Assert.assertEquals(
            "File piece rejected: " + pieceResp.getStatus(),
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            pieceResp.getStatus().getCode());

        return client.pipeTransfer(
            PipeTransferTsFileSealWithModReq.toTPipeTransferReq(
                fileName, tsFile.length(), DATABASE));
      } finally {
        client.closeSession(new TSCloseSessionReq(openSessionResp.getSessionId()));
      }
    }
  }

  private static TSOpenSessionReq buildOpenSessionReq(final String user) {
    final TSOpenSessionReq req = new TSOpenSessionReq();
    req.setClient_protocol(TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3);
    req.setUsername(user);
    req.setPassword(PASSWORD);
    req.setZoneId(ZoneId.systemDefault().toString());
    req.putToConfiguration("version", IoTDBConstant.ClientVersion.V_1_0.toString());
    req.putToConfiguration("sql_dialect", "tree");
    return req;
  }

  private static TPipeTransferReq buildHandshakeReq(
      final String user, final boolean asyncLoadStrategy) throws IOException {
    final Map<String, String> params = new HashMap<>();
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, SENDER_CLUSTER_ID);
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION,
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, user);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, PASSWORD);
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_LOAD_TSFILE_STRATEGY,
        asyncLoadStrategy
            ? PipeSinkConstant.CONNECTOR_LOAD_TSFILE_STRATEGY_ASYNC_VALUE
            : PipeSinkConstant.CONNECTOR_LOAD_TSFILE_STRATEGY_SYNC_VALUE);
    return PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(params);
  }

  private static void assertSealAccepted(
      final File tsFile, final String user, final boolean asyncLoadStrategy) throws Exception {
    final TPipeTransferResp resp = sendTsFile(tsFile, user, asyncLoadStrategy);
    Assert.assertEquals(
        "The asynchronous load should be accepted and authorized when it is executed: "
            + resp.getStatus(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        resp.getStatus().getCode());
  }

  private static void assertSealRejected(
      final File tsFile, final String user, final boolean asyncLoadStrategy) throws Exception {
    final TPipeTransferResp resp = sendTsFile(tsFile, user, asyncLoadStrategy);
    Assert.assertNotEquals(
        "The load of an account without grants must not be accepted: " + resp.getStatus(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        resp.getStatus().getCode());
  }

  private static void waitUntilActiveLoadIsIdle(final long timeoutMs) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      boolean hasTsFile = false;
      for (final DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
        if (containsTsFile(getActiveLoadPendingDir(dataNode))) {
          hasTsFile = true;
          break;
        }
      }
      if (!hasTsFile) {
        return;
      }
      Thread.sleep(500L);
    }
    Assert.fail("Timed out waiting for the active load pending directories to become empty");
  }

  private static File getActiveLoadPendingDir(final DataNodeWrapper dataNode) {
    return new File(
        dataNode.getNodePath()
            + File.separator
            + "ext"
            + File.separator
            + "load"
            + File.separator
            + "pending");
  }

  private static boolean containsTsFile(final File root) {
    if (root == null || !root.exists()) {
      return false;
    }
    if (root.isFile()) {
      return root.getName().endsWith(".tsfile");
    }

    final File[] children = root.listFiles();
    if (children == null) {
      return false;
    }
    for (final File child : children) {
      if (containsTsFile(child)) {
        return true;
      }
    }
    return false;
  }

  private static void assertCount(final String measurement, final long expected) throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet =
            statement.executeQuery(
                String.format("select count(%s) from %s", measurement, DEVICE))) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(
          "Unexpected number of points written to " + measurement, expected, resultSet.getLong(1));
    }
  }
}
