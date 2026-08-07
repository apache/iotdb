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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV1Req;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.sink.payload.legacy.PipeData;
import org.apache.iotdb.db.pipe.sink.payload.legacy.TsFilePipeData;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.Deletion;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBPipeReceiverIT {

  private static final String DATA_NODE_DEVICE = "root.pipe_receiver_session.d1";

  private static final String LEGACY_PIPE_USER = "pipeHack";
  private static final String LEGACY_PIPE_PASSWORD = "StrngPsWd@623451";
  private static final String LEGACY_DATABASE = "root.legacy_poc";
  private static final String LEGACY_TIMESERIES = LEGACY_DATABASE + ".d1.s1";

  private static final String NO_USE_PIPE_USER = "legacyNoUsePipe";
  private static final String NO_USE_PIPE_PASSWORD = "StrngPsWd@623452";

  private static final String LEGACY_TSFILE_USER = "legacyTsFileUser";
  private static final String LEGACY_TSFILE_PASSWORD = "StrngPsWd@623453";
  private static final String LEGACY_TSFILE_DATABASE = "root.legacy_tsfile_auth";
  private static final String LEGACY_TSFILE_DEVICE = LEGACY_TSFILE_DATABASE + ".d1";
  private static final String LEGACY_TSFILE_NAME =
      "0-" + LEGACY_TSFILE_DATABASE + "-0-0-0-0-0-0.tsfile";

  private static final String NO_AUTO_CREATE_DATABASE = "root.legacy_no_auto_create";
  private static final String NO_AUTO_CREATE_DEVICE = NO_AUTO_CREATE_DATABASE + ".d1";
  private static final String NO_AUTO_CREATE_TSFILE_NAME =
      "0-" + NO_AUTO_CREATE_DATABASE + "-0-0-0-0-0-0.tsfile";

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDataNodeReceiverSessionHandling() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.pipe_receiver_session");
      statement.execute(
          "CREATE TIMESERIES " + DATA_NODE_DEVICE + ".s1 WITH DATATYPE=INT32,ENCODING=RLE");
    }

    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    try (final IoTDBSyncClient client = createClient(dataNode)) {
      Assert.assertEquals(
          TSStatusCode.NOT_LOGIN.getStatusCode(),
          client.pipeTransfer(buildTabletReq(1, 1)).getStatus().getCode());
      Assert.assertEquals(
          TSStatusCode.NOT_LOGIN.getStatusCode(),
          client.getBackupConfiguration().getStatus().getCode());
      Assert.assertTrue(client.fetchAllConnectionsInfo().getConnectionInfoList().isEmpty());
      Assert.assertEquals(
          TSStatusCode.NOT_LOGIN.getStatusCode(),
          client
              .pipeSubscribe(new TPipeSubscribeReq().setVersion((byte) 1).setType((short) 0))
              .getStatus()
              .getCode());

      Assert.assertEquals(
          TSStatusCode.PIPE_HANDSHAKE_ERROR.getStatusCode(),
          client
              .pipeTransfer(
                  PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(
                      CommonDescriptor.getInstance().getConfig().getTimestampPrecision()))
              .getStatus()
              .getCode());
      Assert.assertEquals(
          TSStatusCode.NOT_LOGIN.getStatusCode(),
          client
              .pipeTransfer(
                  PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(
                      buildHandshakeParams(null, null)))
              .getStatus()
              .getCode());

      final TPipeTransferResp wrongPasswordResp =
          client.pipeTransfer(
              PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(
                  buildHandshakeParams(SessionConfig.DEFAULT_USER, "wrong-password")));
      Assert.assertNotEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), wrongPasswordResp.getStatus().getCode());
      Assert.assertEquals(
          TSStatusCode.NOT_LOGIN.getStatusCode(),
          client.pipeTransfer(buildTabletReq(1, 1)).getStatus().getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .pipeTransfer(
                  PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(
                      buildHandshakeParams(
                          SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD)))
              .getStatus()
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.pipeTransfer(buildTabletReq(1, 1)).getStatus().getCode());
    }

    try (final IoTDBSyncClient client = createClient(dataNode)) {
      final TSOpenSessionResp openSessionResp = client.openSession(createOpenSessionReq());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), openSessionResp.getStatus().getCode());
      try {
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            client.getBackupConfiguration().getStatus().getCode());
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            client.pipeTransfer(buildTabletReq(2, 2)).getStatus().getCode());
      } finally {
        client.closeSession(new TSCloseSessionReq(openSessionResp.getSessionId()));
      }
    }

    assertTimeseriesRowCount(DATA_NODE_DEVICE, "s1", 2);
  }

  @Test
  public void testConfigNodeReceiverSessionHandling() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    try (final IoTDBSyncClient client = createClient(dataNode)) {
      Assert.assertEquals(
          TSStatusCode.PIPE_CONFIG_RECEIVER_HANDSHAKE_NEEDED.getStatusCode(),
          client.pipeTransfer(buildEmptyConfigPlanReq()).getStatus().getCode());
      Assert.assertEquals(
          TSStatusCode.PIPE_HANDSHAKE_ERROR.getStatusCode(),
          client
              .pipeTransfer(
                  PipeTransferConfigNodeHandshakeV1Req.toTPipeTransferReq(
                      CommonDescriptor.getInstance().getConfig().getTimestampPrecision()))
              .getStatus()
              .getCode());
      Assert.assertEquals(
          TSStatusCode.NOT_LOGIN.getStatusCode(),
          client
              .pipeTransfer(
                  PipeTransferConfigNodeHandshakeV2Req.toTPipeTransferReq(
                      buildHandshakeParams(null, null)))
              .getStatus()
              .getCode());

      final TPipeTransferResp wrongPasswordResp =
          client.pipeTransfer(
              PipeTransferConfigNodeHandshakeV2Req.toTPipeTransferReq(
                  buildHandshakeParams(SessionConfig.DEFAULT_USER, "wrong-password")));
      Assert.assertNotEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), wrongPasswordResp.getStatus().getCode());
      Assert.assertEquals(
          TSStatusCode.PIPE_CONFIG_RECEIVER_HANDSHAKE_NEEDED.getStatusCode(),
          client.pipeTransfer(buildEmptyConfigPlanReq()).getStatus().getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .pipeTransfer(
                  PipeTransferConfigNodeHandshakeV2Req.toTPipeTransferReq(
                      buildHandshakeParams(
                          SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD)))
              .getStatus()
              .getCode());
      Assert.assertNotEquals(
          TSStatusCode.NOT_LOGIN.getStatusCode(),
          client.pipeTransfer(buildEmptyConfigPlanReq()).getStatus().getCode());
    }
  }

  @Test
  public void testRejectPathTraversalFileNameInLegacyTransportFile() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);

    try (final IoTDBSyncClient client =
        new IoTDBSyncClient(
            new ThriftClientProperty.Builder().build(),
            dataNode.getIp(),
            dataNode.getPort(),
            false,
            null,
            null)) {
      final TSOpenSessionResp openSessionResp = client.openSession(createOpenSessionReq());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), openSessionResp.getStatus().getCode());

      try {
        final TSStatus handshakeStatus =
            client.handshake(
                new TSyncIdentityInfo(
                    "pathTraversalPipe", System.currentTimeMillis(), "UNKNOWN", ""));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), handshakeStatus.getCode());

        final String maliciousFileName =
            ".." + File.separator + ".." + File.separator + "pwned.tsfile";
        final TSStatus status =
            client.sendFile(
                new TSyncTransportMetaInfo(maliciousFileName, 0),
                ByteBuffer.wrap("pwned".getBytes(StandardCharsets.UTF_8)));

        Assert.assertEquals(TSStatusCode.SYNC_FILE_ERROR.getStatusCode(), status.getCode());
        Assert.assertTrue(status.getMessage().contains("Illegal fileName"));
      } finally {
        client.closeSession(new TSCloseSessionReq(openSessionResp.getSessionId()));
      }
    }
  }

  @Test
  public void testLegacyPipeRpcRequiresLoginAndUsePipePrivilege() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER " + NO_USE_PIPE_USER + " '" + NO_USE_PIPE_PASSWORD + "'");
    }

    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    try (final IoTDBSyncClient client = createClient(dataNode)) {
      assertLegacyPipeRpcStatus(client, TSStatusCode.NOT_LOGIN);

      final TSOpenSessionResp openSessionResp =
          client.openSession(createOpenSessionReq(NO_USE_PIPE_USER, NO_USE_PIPE_PASSWORD));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), openSessionResp.getStatus().getCode());
      try {
        assertLegacyPipeRpcStatus(client, TSStatusCode.NO_PERMISSION);
      } finally {
        client.closeSession(new TSCloseSessionReq(openSessionResp.getSessionId()));
      }
    }
  }

  @Test
  public void testLegacyPipeDataDeleteUsesAuthenticatedUserPermission() throws Exception {
    prepareLegacyPipePrivilegeEscalationData();
    assertDirectDeleteDeniedForLegacyPipeUser();

    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    try (final IoTDBSyncClient client =
        new IoTDBSyncClient(
            new ThriftClientProperty.Builder().build(),
            dataNode.getIp(),
            dataNode.getPort(),
            false,
            null,
            null)) {
      final TSOpenSessionResp openSessionResp =
          client.openSession(createOpenSessionReq(LEGACY_PIPE_USER, LEGACY_PIPE_PASSWORD));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), openSessionResp.getStatus().getCode());

      try {
        final TSStatus handshakeStatus =
            client.handshake(
                new TSyncIdentityInfo(
                    "legacyPipePrivilege", System.currentTimeMillis(), "UNKNOWN", ""));
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), handshakeStatus.getCode());

        final TSStatus status =
            client.sendPipeData(ByteBuffer.wrap(createDeletionPipeDataPayload()));

        Assert.assertEquals(TSStatusCode.PIPESERVER_ERROR.getStatusCode(), status.getCode());
      } finally {
        client.closeSession(new TSCloseSessionReq(openSessionResp.getSessionId()));
      }
    }

    assertLegacyPocRowCount(2);
  }

  @Test
  public void testLegacyTsFileLoadUsesAuthenticatedUserPermission() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + LEGACY_TSFILE_DATABASE);
      statement.execute(
          "CREATE TIMESERIES " + LEGACY_TSFILE_DEVICE + ".s1 WITH DATATYPE=INT32,ENCODING=RLE");
      statement.execute("CREATE USER " + LEGACY_TSFILE_USER + " '" + LEGACY_TSFILE_PASSWORD + "'");
      statement.execute("GRANT SYSTEM ON root.** TO USER " + LEGACY_TSFILE_USER);
    }

    final File tempDir = Files.createTempDirectory("legacy-pipe-tsfile-auth").toFile();
    try {
      final File tsFile = new File(tempDir, LEGACY_TSFILE_NAME);
      generateTsFile(tsFile, LEGACY_TSFILE_DEVICE);

      final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
      try (final IoTDBSyncClient client = createClient(dataNode)) {
        final TSOpenSessionResp openSessionResp =
            client.openSession(createOpenSessionReq(LEGACY_TSFILE_USER, LEGACY_TSFILE_PASSWORD));
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), openSessionResp.getStatus().getCode());

        try {
          final TSStatus handshakeStatus =
              client.handshake(
                  new TSyncIdentityInfo(
                      "legacyTsFilePrivilege",
                      System.currentTimeMillis(),
                      "UNKNOWN",
                      LEGACY_TSFILE_DATABASE));
          Assert.assertEquals(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(), handshakeStatus.getCode());

          final TSStatus status = sendLegacyTsFile(client, tsFile);
          Assert.assertEquals(TSStatusCode.PIPESERVER_ERROR.getStatusCode(), status.getCode());
        } finally {
          client.closeSession(new TSCloseSessionReq(openSessionResp.getSessionId()));
        }
      }
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }

    assertTimeseriesRowCount(LEGACY_TSFILE_DEVICE, "s1", 0);
  }

  @Test
  public void testLegacyHandshakeAndTsFileLoadRespectDisabledAutoCreate() throws Exception {
    final File tempDir = Files.createTempDirectory("legacy-pipe-no-auto-create").toFile();
    try {
      final File tsFile = new File(tempDir, NO_AUTO_CREATE_TSFILE_NAME);
      generateTsFile(tsFile, NO_AUTO_CREATE_DEVICE);

      final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
      try (final IoTDBSyncClient client = createClient(dataNode)) {
        final TSOpenSessionResp openSessionResp = client.openSession(createOpenSessionReq());
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), openSessionResp.getStatus().getCode());

        try {
          final TSStatus handshakeStatus =
              client.handshake(
                  new TSyncIdentityInfo(
                      "legacyNoAutoCreate",
                      System.currentTimeMillis(),
                      "UNKNOWN",
                      NO_AUTO_CREATE_DATABASE));
          Assert.assertEquals(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(), handshakeStatus.getCode());
          assertDatabaseDoesNotExist(NO_AUTO_CREATE_DATABASE);

          final TSStatus loadStatus = sendLegacyTsFile(client, tsFile);
          Assert.assertEquals(TSStatusCode.PIPESERVER_ERROR.getStatusCode(), loadStatus.getCode());
          assertDatabaseDoesNotExist(NO_AUTO_CREATE_DATABASE);
        } finally {
          client.closeSession(new TSCloseSessionReq(openSessionResp.getSessionId()));
        }
      }
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  private TPipeTransferReq buildEmptyConfigPlanReq() {
    return new TPipeTransferReq()
        .setVersion(IoTDBSinkRequestVersion.VERSION_1.getVersion())
        .setType(PipeRequestType.TRANSFER_CONFIG_PLAN.getType())
        .setBody(ByteBuffer.allocate(0));
  }

  private TPipeTransferReq buildTabletReq(final long timestamp, final int value) throws Exception {
    final Tablet tablet =
        new Tablet(
            DATA_NODE_DEVICE,
            Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32)),
            1);
    tablet.addTimestamp(0, timestamp);
    tablet.addValue("s1", 0, value);
    return PipeTransferTabletRawReq.toTPipeTransferReq(tablet, false);
  }

  private Map<String, String> buildHandshakeParams(final String username, final String password) {
    final Map<String, String> params = new HashMap<>();
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID,
        "pipe-session-it-" + UUID.randomUUID());
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION,
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    if (username != null) {
      params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, username);
    }
    if (password != null) {
      params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, password);
    }
    return params;
  }

  private void assertLegacyPipeRpcStatus(
      final IoTDBSyncClient client, final TSStatusCode expectedStatusCode) throws Exception {
    final int expectedCode = expectedStatusCode.getStatusCode();
    Assert.assertEquals(
        expectedCode,
        client
            .handshake(
                new TSyncIdentityInfo(
                    "legacyRpcPermission", System.currentTimeMillis(), "UNKNOWN", ""))
            .getCode());
    Assert.assertEquals(
        expectedCode,
        client
            .sendFile(
                new TSyncTransportMetaInfo("permission.tsfile", 0), ByteBuffer.wrap(new byte[] {1}))
            .getCode());
    Assert.assertEquals(expectedCode, client.sendPipeData(ByteBuffer.allocate(0)).getCode());
  }

  private void prepareLegacyPipePrivilegeEscalationData() throws SQLException {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + LEGACY_DATABASE);
      statement.execute(
          "CREATE TIMESERIES " + LEGACY_TIMESERIES + " WITH DATATYPE=INT64,ENCODING=PLAIN");
      statement.execute("INSERT INTO root.legacy_poc.d1(time,s1) VALUES (1,1),(2,2)");
      statement.execute("CREATE USER " + LEGACY_PIPE_USER + " '" + LEGACY_PIPE_PASSWORD + "'");
      statement.execute("GRANT SYSTEM ON root.** TO USER " + LEGACY_PIPE_USER);
    }
  }

  private void assertDirectDeleteDeniedForLegacyPipeUser() throws SQLException {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(LEGACY_PIPE_USER, LEGACY_PIPE_PASSWORD);
        final Statement statement = connection.createStatement()) {
      final SQLException exception =
          Assert.assertThrows(
              SQLException.class,
              () -> statement.execute("DELETE FROM " + LEGACY_TIMESERIES + " WHERE time <= 1"));
      Assert.assertTrue(exception.getMessage().contains("WRITE_DATA"));
    }
  }

  private byte[] createDeletionPipeDataPayload() throws Exception {
    final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    try (final DataOutputStream stream = new DataOutputStream(byteStream)) {
      stream.writeByte(PipeData.PipeDataType.DELETION.getType());
      stream.writeLong(1L);
      ReadWriteIOUtils.write(LEGACY_DATABASE, stream);
      new Deletion(new MeasurementPath(LEGACY_TIMESERIES), 0, Long.MIN_VALUE, 1)
          .serializeWithoutFileOffset(stream);
    }
    return byteStream.toByteArray();
  }

  private void assertLegacyPocRowCount(final int expectedCount) throws SQLException {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.legacy_poc.d1")) {
      int actualCount = 0;
      while (resultSet.next()) {
        ++actualCount;
      }
      Assert.assertEquals(expectedCount, actualCount);
    }
  }

  private void assertTimeseriesRowCount(
      final String device, final String measurement, final int expectedCount) throws SQLException {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet =
            statement.executeQuery("SELECT COUNT(" + measurement + ") FROM " + device)) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(expectedCount, resultSet.getInt(1));
    }
  }

  private void assertDatabaseDoesNotExist(final String database) throws SQLException {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
      while (resultSet.next()) {
        Assert.assertNotEquals(database, resultSet.getString(1));
      }
    }
  }

  private void generateTsFile(final File tsFile, final String device) throws Exception {
    try (final TsFileGenerator generator = new TsFileGenerator(tsFile)) {
      generator.registerTimeseries(
          device,
          Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE)));
      generator.generateData(device, 2, 1, false);
    }
  }

  private TSStatus sendLegacyTsFile(final IoTDBSyncClient client, final File tsFile)
      throws Exception {
    final TSStatus fileStatus =
        client.sendFile(
            new TSyncTransportMetaInfo(tsFile.getName(), 0),
            ByteBuffer.wrap(Files.readAllBytes(tsFile.toPath())));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), fileStatus.getCode());
    return client.sendPipeData(
        ByteBuffer.wrap(new TsFilePipeData("", tsFile.getName(), 1).serialize()));
  }

  private IoTDBSyncClient createClient(final DataNodeWrapper dataNode) throws Exception {
    return new IoTDBSyncClient(
        new ThriftClientProperty.Builder().build(),
        dataNode.getIp(),
        dataNode.getPort(),
        false,
        null,
        null);
  }

  private TSOpenSessionReq createOpenSessionReq() {
    return createOpenSessionReq(SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
  }

  private TSOpenSessionReq createOpenSessionReq(final String username, final String password) {
    final TSOpenSessionReq req = new TSOpenSessionReq();
    req.setClient_protocol(TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3);
    req.setUsername(username);
    req.setPassword(password);
    req.setZoneId(ZoneId.systemDefault().toString());
    req.putToConfiguration("version", IoTDBConstant.ClientVersion.V_1_0.toString());
    req.putToConfiguration("sql_dialect", "tree");
    return req;
  }
}
