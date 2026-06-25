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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBAutoResizingBufferMemoryIT {

  private static final int DATANODE_MAX_HEAP_SIZE_IN_MB = 256;
  private static final int AUTO_RESIZING_BUFFER_COUNT_PER_CONNECTION = 2;
  private static final int CONNECTION_COUNT_OVERFLOW_MARGIN = 1;
  private static final int INITIAL_GROWING_REQUEST_PAYLOAD_SIZE = 16 * 1024;
  private static final int MAX_GROWING_REQUEST_PAYLOAD_SIZE =
      calculateNextPowerOfTwo(calculateAutoResizingBufferMemorySizeInBytes());

  @BeforeClass
  public static void setUp() throws Exception {
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

  @Test
  public void testGrowingRequestsAreRejectedWhenBufferMemoryIsExhausted() throws Exception {
    boolean rejected = false;

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.auto_resizing_buffer_growing_request");
      statement.execute(
          "CREATE TIMESERIES root.auto_resizing_buffer_growing_request.d1.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN");
    }

    // do not use connection because its inevitable retry will slow down the test
    try (ThriftClientContext clientContext = ThriftClientContext.open()) {
      int payloadSize = INITIAL_GROWING_REQUEST_PAYLOAD_SIZE;
      while (payloadSize <= MAX_GROWING_REQUEST_PAYLOAD_SIZE) {
        try {
          clientContext.executeStatement(
              String.format(
                  "INSERT INTO root.auto_resizing_buffer_growing_request.d1(time, s1) VALUES (%d, '%s')",
                  payloadSize, repeat('a', payloadSize)));
        } catch (Exception e) {
          rejected = true;
          clientContext.markBroken();
          break;
        }
        payloadSize = payloadSize << 1;
      }
    }

    Assert.assertTrue(
        "Expected a growing request to be rejected after AutoResizingBuffer memory is exhausted",
        rejected);
  }

  private static void closeQuietly(AutoCloseable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Exception ignored) {
      // ignored
    }
  }

  private static int calculateConnectionCountToExhaustAutoResizingBufferMemory() {
    int autoResizingBufferInitialSizePerConnection =
        AUTO_RESIZING_BUFFER_COUNT_PER_CONNECTION * RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY;
    return (int)
        (calculateAutoResizingBufferMemorySizeInBytes() / autoResizingBufferInitialSizePerConnection
            + CONNECTION_COUNT_OVERFLOW_MARGIN);
  }

  private static long calculateAutoResizingBufferMemorySizeInBytes() {
    return (DATANODE_MAX_HEAP_SIZE_IN_MB * 1024L * 1024L * 5 / 100); // 5% of the max heap size;
  }

  private static int calculateNextPowerOfTwo(long value) {
    int result = 1;
    while (result < value) {
      result <<= 1;
    }
    return result;
  }

  private static String repeat(char character, int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, character);
    return new String(chars);
  }

  private static TSOpenSessionReq createOpenSessionReq() {
    TSOpenSessionReq req = new TSOpenSessionReq();
    req.setUsername(SessionConfig.DEFAULT_USER);
    req.setPassword(SessionConfig.DEFAULT_PASSWORD);
    req.setZoneId(ZoneId.systemDefault().toString());
    req.putToConfiguration("version", IoTDBConstant.ClientVersion.V_1_0.toString());
    req.putToConfiguration("sql_dialect", "tree");
    return req;
  }

  private static class ThriftClientContext implements AutoCloseable {

    private final TTransport transport;
    private final IClientRPCService.Client client;
    private final long sessionId;
    private final long statementId;
    private boolean broken;

    private ThriftClientContext(
        TTransport transport, IClientRPCService.Client client, long sessionId, long statementId) {
      this.transport = transport;
      this.client = client;
      this.sessionId = sessionId;
      this.statementId = statementId;
    }

    private static ThriftClientContext open() throws Exception {
      DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
      TTransport transport =
          DeepCopyRpcTransportFactory.INSTANCE.getTransport(
              dataNode.getIp(), dataNode.getPort(), 0);
      transport.open();
      IClientRPCService.Client client =
          new IClientRPCService.Client(new TBinaryProtocol(transport));
      TSOpenSessionResp openSessionResp = client.openSession(createOpenSessionReq());
      RpcUtils.verifySuccess(openSessionResp.getStatus());
      long sessionId = openSessionResp.getSessionId();
      return new ThriftClientContext(
          transport, client, sessionId, client.requestStatementId(sessionId));
    }

    private void executeStatement(String sql) throws Exception {
      TSExecuteStatementResp resp =
          client.executeStatementV2(new TSExecuteStatementReq(sessionId, sql, statementId));
      RpcUtils.verifySuccess(resp.getStatus());
    }

    private void markBroken() {
      broken = true;
    }

    @Override
    public void close() throws Exception {
      try {
        if (!broken) {
          client.closeOperation(new TSCloseOperationReq(sessionId).setStatementId(statementId));
          client.closeSession(new TSCloseSessionReq(sessionId));
        }
      } finally {
        transport.close();
      }
    }
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
