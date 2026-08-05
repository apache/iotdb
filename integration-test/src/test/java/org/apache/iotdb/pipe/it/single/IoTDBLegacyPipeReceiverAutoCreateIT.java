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
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.db.pipe.sink.payload.legacy.TsFilePipeData;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Collections;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBLegacyPipeReceiverAutoCreateIT {

  private static final String DATABASE = "root.legacy_no_auto_create";
  private static final String DEVICE = DATABASE + ".d1";
  private static final String TSFILE_NAME = "0-" + DATABASE + "-0-0-0-0-0-0.tsfile";

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDatanodeMemoryProportion("3:3:1:1:1:0");
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testLegacyHandshakeAndTsFileLoadRespectDisabledAutoCreate() throws Exception {
    final File tempDir = Files.createTempDirectory("legacy-pipe-no-auto-create").toFile();
    try {
      final File tsFile = new File(tempDir, TSFILE_NAME);
      try (final TsFileGenerator generator = new TsFileGenerator(tsFile)) {
        generator.registerTimeseries(
            DEVICE,
            Collections.singletonList(
                new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE)));
        generator.generateData(DEVICE, 2, 1, false);
      }

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
                      "legacyNoAutoCreate", System.currentTimeMillis(), "UNKNOWN", DATABASE));
          Assert.assertEquals(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(), handshakeStatus.getCode());
          assertDatabaseDoesNotExist();

          final TSStatus fileStatus =
              client.sendFile(
                  new TSyncTransportMetaInfo(tsFile.getName(), 0),
                  ByteBuffer.wrap(Files.readAllBytes(tsFile.toPath())));
          Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), fileStatus.getCode());

          final TSStatus loadStatus =
              client.sendPipeData(
                  ByteBuffer.wrap(new TsFilePipeData("", tsFile.getName(), 1).serialize()));
          Assert.assertEquals(TSStatusCode.PIPESERVER_ERROR.getStatusCode(), loadStatus.getCode());
          assertDatabaseDoesNotExist();
        } finally {
          client.closeSession(new TSCloseSessionReq(openSessionResp.getSessionId()));
        }
      }
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  private void assertDatabaseDoesNotExist() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
      while (resultSet.next()) {
        Assert.assertNotEquals(DATABASE, resultSet.getString(1));
      }
    }
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
}
