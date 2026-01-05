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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.client.ClientRunner;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.client.IoTDBOpcUaClient;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT1;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.client.api.identity.UsernameProvider;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.ConnectException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace.timestampToUtc;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT1.class})
public class IoTDBPipeOPCUAIT extends AbstractPipeSingleIT {

  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(1);
    env = MultiEnvFactory.getEnv(0);
    env.getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    env.initClusterEnvironment(1, 1);
  }

  @Test
  public void testOPCUAServerSink() throws Exception {
    int tcpPort = -1;
    int httpsPort = -1;
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQuery(env, "insert into root.db.d1(time, s1) values (1, 1)", null);

      final Map<String, String> sinkAttributes = new HashMap<>();

      sinkAttributes.put("sink", "opc-ua-sink");
      sinkAttributes.put("opcua.model", "client-server");
      sinkAttributes.put("security-policy", "None");

      OpcUaClient opcUaClient;
      DataValue value;
      while (true) {
        final int[] ports = EnvUtils.searchAvailablePorts();
        tcpPort = ports[0];
        httpsPort = ports[1];
        sinkAttributes.put("tcp.port", Integer.toString(tcpPort));
        sinkAttributes.put("https.port", Integer.toString(httpsPort));

        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            client
                .createPipe(
                    new TCreatePipeReq("testPipe", sinkAttributes)
                        .setExtractorAttributes(Collections.singletonMap("user", "root"))
                        .setProcessorAttributes(Collections.emptyMap()))
                .getCode());

        try {
          opcUaClient =
              getOpcUaClient(
                  "opc.tcp://127.0.0.1:" + tcpPort + "/iotdb", SecurityPolicy.None, "root", "root");
        } catch (final PipeException e) {
          if (e.getCause() instanceof ConnectException) {
            continue;
          } else {
            throw e;
          }
        }
        value =
            opcUaClient.readValue(0, TimestampsToReturn.Both, new NodeId(2, "root/db/d1/s1")).get();
        Assert.assertEquals(new Variant(1.0), value.getValue());
        Assert.assertEquals(new DateTime(timestampToUtc(1)), value.getSourceTime());
        opcUaClient.disconnect().get();
        break;
      }

      // Create the region first to avoid tsFile parsing
      TestUtils.executeNonQueries(
          env,
          Arrays.asList(
              "create aligned timeSeries root.db.opc(value double, quality boolean, other int32)",
              "insert into root.db.opc(time, value, quality, other) values (0, 0, true, 1)"),
          null);

      while (true) {
        final int[] ports = EnvUtils.searchAvailablePorts();
        tcpPort = ports[0];
        httpsPort = ports[1];
        sinkAttributes.put("tcp.port", Integer.toString(tcpPort));
        sinkAttributes.put("https.port", Integer.toString(httpsPort));
        sinkAttributes.put("with-quality", "true");

        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            client
                .alterPipe(
                    new TAlterPipeReq()
                        .setPipeName("testPipe")
                        .setIsReplaceAllConnectorAttributes(true)
                        .setConnectorAttributes(sinkAttributes)
                        .setProcessorAttributes(Collections.emptyMap())
                        .setExtractorAttributes(Collections.emptyMap()))
                .getCode());
        try {
          opcUaClient =
              getOpcUaClient(
                  "opc.tcp://127.0.0.1:" + tcpPort + "/iotdb", SecurityPolicy.None, "root", "root");
        } catch (final PipeException e) {
          if (e.getCause() instanceof ConnectException) {
            continue;
          } else {
            throw e;
          }
        }
        break;
      }

      TestUtils.executeNonQuery(
          env,
          "insert into root.db.opc(time, value, quality, other) values (1, 1, false, 1)",
          null);

      long startTime = System.currentTimeMillis();
      while (true) {
        try {
          value =
              opcUaClient.readValue(0, TimestampsToReturn.Both, new NodeId(2, "root/db/opc")).get();
          Assert.assertEquals(new Variant(1.0), value.getValue());
          Assert.assertEquals(StatusCode.BAD, value.getStatusCode());
          Assert.assertEquals(new DateTime(timestampToUtc(1)), value.getSourceTime());
          break;
        } catch (final Throwable t) {
          if (System.currentTimeMillis() - startTime > 10_000L) {
            throw t;
          }
        }
      }

      TestUtils.executeNonQuery(
          env, "insert into root.db.opc(time, quality) values (2, true)", null);
      TestUtils.executeNonQuery(env, "insert into root.db.opc(time, value) values (2, 2)", null);

      startTime = System.currentTimeMillis();
      while (true) {
        try {
          value =
              opcUaClient.readValue(0, TimestampsToReturn.Both, new NodeId(2, "root/db/opc")).get();
          Assert.assertEquals(new DateTime(timestampToUtc(2)), value.getSourceTime());
          Assert.assertEquals(new Variant(2.0), value.getValue());
          Assert.assertEquals(StatusCode.UNCERTAIN, value.getStatusCode());
          break;
        } catch (final Throwable t) {
          if (System.currentTimeMillis() - startTime > 10_000L) {
            throw t;
          }
        }
      }

      opcUaClient.disconnect().get();
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("testPipe").getCode());

      // Test reconstruction
      sinkAttributes.put("password", "test");
      sinkAttributes.put("security-policy", "basic256sha256");
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(Collections.emptyMap())
                      .setProcessorAttributes(Collections.emptyMap()))
              .getCode());

      // Banned none, only allows basic256sha256
      final int finalTcpPort = tcpPort;
      Assert.assertThrows(
          PipeException.class,
          () ->
              getOpcUaClient(
                  "opc.tcp://127.0.0.1:" + finalTcpPort + "/iotdb",
                  SecurityPolicy.None,
                  "root",
                  "root"));

      // Test conflict
      sinkAttributes.put("password", "conflict");
      try {
        TestUtils.executeNonQuery(
            env,
            String.format(
                "create pipe test1 ('sink'='opc-ua-sink', 'password'='conflict@pswd', 'tcp.port'='%s', 'https.port'='%s')",
                tcpPort, httpsPort),
            null);
        Assert.fail();
      } catch (final Exception e) {
        Assert.assertEquals(
            String.format(
                "org.apache.iotdb.jdbc.IoTDBSQLException: 1107: The existing server with tcp port %s and https port %s's password **** conflicts to the new password ****, reject reusing.",
                tcpPort, httpsPort),
            e.getMessage());
      }
    } finally {
      if (tcpPort >= 0) {
        final String lockPath = EnvUtils.getLockFilePath(tcpPort);
        if (!new File(lockPath).delete()) {
          System.out.printf("Delete lock file %s failed%n", lockPath);
        }
      }
    }
  }

  @Test
  public void testOPCUASinkInTableModel() throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(env, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 10, env);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "opc-ua-sink");
      sinkAttributes.put("opcua.model", "client-server");

      final int[] ports = EnvUtils.searchAvailablePorts();
      final int tcpPort = ports[0];
      final int httpsPort = ports[1];
      sinkAttributes.put("tcp.port", Integer.toString(tcpPort));
      sinkAttributes.put("https.port", Integer.toString(httpsPort));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes)
                      .setProcessorAttributes(Collections.emptyMap()))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("testPipe").getCode());

      // Test reconstruction
      sinkAttributes.put("password123456", "test");
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(Collections.emptyMap())
                      .setProcessorAttributes(Collections.emptyMap()))
              .getCode());

      // Test conflict
      sinkAttributes.put("password123456", "conflict");
      Assert.assertEquals(
          TSStatusCode.PIPE_ERROR.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(Collections.emptyMap())
                      .setProcessorAttributes(Collections.emptyMap()))
              .getCode());
    }
  }

  private static OpcUaClient getOpcUaClient(
      final String nodeUrl,
      final SecurityPolicy policy,
      final String userName,
      final String password) {
    final IoTDBOpcUaClient client;

    final IdentityProvider provider =
        Objects.nonNull(userName)
            ? new UsernameProvider(userName, password)
            : new AnonymousProvider();

    final String securityDir =
        CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE
            + File.separatorChar
            + UUID.nameUUIDFromBytes(nodeUrl.getBytes(TSFileConfig.STRING_CHARSET));

    client = new IoTDBOpcUaClient(nodeUrl, policy, provider, false);
    new ClientRunner(client, securityDir, password).run();
    return client.getClient();
  }
}
