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

import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV1Req;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBPipeReceiverSessionIT {

  private static final String DATA_NODE_DEVICE = "root.pipe_receiver_session.d1";

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
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

    assertRowCount(DATA_NODE_DEVICE, 2);
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
    final TSOpenSessionReq req = new TSOpenSessionReq();
    req.setClient_protocol(TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3);
    req.setUsername(SessionConfig.DEFAULT_USER);
    req.setPassword(SessionConfig.DEFAULT_PASSWORD);
    req.setZoneId(ZoneId.systemDefault().toString());
    req.putToConfiguration("version", IoTDBConstant.ClientVersion.V_1_0.toString());
    req.putToConfiguration("sql_dialect", "tree");
    return req;
  }

  private void assertRowCount(final String device, final int expectedCount) throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM " + device)) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(expectedCount, resultSet.getInt(1));
    }
  }
}
