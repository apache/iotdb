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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.db.pipe.sink.payload.legacy.PipeData;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.Deletion;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;

import org.apache.tsfile.utils.ReadWriteIOUtils;
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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBLegacyPipeReceiverSecurityIT {

  private static final String LEGACY_PIPE_USER = "pipeHack";
  private static final String LEGACY_PIPE_PASSWORD = "StrngPsWd@623451";
  private static final String LEGACY_DATABASE = "root.legacy_poc";
  private static final String LEGACY_TIMESERIES = LEGACY_DATABASE + ".d1.s1";

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDatanodeMemoryProportion("3:3:1:1:1:0");
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
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
