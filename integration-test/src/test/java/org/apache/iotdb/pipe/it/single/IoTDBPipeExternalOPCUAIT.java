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
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.client.ClientRunner;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.client.IoTDBOpcUaClient;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaServerBuilder;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT1;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.core.Reference;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.api.services.NodeManagementServices.AddNodesContext;
import org.eclipse.milo.opcua.sdk.server.nodes.UaFolderNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExtensionObject;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesItem;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesResult;
import org.eclipse.milo.opcua.stack.core.types.structured.ObjectAttributes;
import org.eclipse.milo.opcua.stack.core.types.structured.VariableAttributes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace.timestampToUtc;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT1.class})
public class IoTDBPipeExternalOPCUAIT extends AbstractPipeSingleIT {

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
  public void testOPCUASinkWritesToExternalServerWithAddNodes() throws Exception {
    int tcpPort = -1;
    ExternalAddNodesOpcUaServer externalServer = null;
    OpcUaClient opcUaClient = null;
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQuery(
          env,
          "create aligned timeSeries root.db.ext(value double, quality boolean, other int32)",
          null);
      TestUtils.executeNonQuery(
          env, "create aligned timeSeries root.db.ready(value double, quality boolean)", null);
      TestUtils.executeNonQuery(
          env, "insert into root.db.ready(time, value, quality) values (0, 0, true)", null);

      final int[] ports = EnvUtils.searchAvailablePorts();
      tcpPort = ports[0];
      final int httpsPort = ports[1];
      externalServer = ExternalAddNodesOpcUaServer.start(tcpPort, httpsPort);

      final String nodeUrl = "opc.tcp://127.0.0.1:" + tcpPort + "/iotdb";
      final Map<String, String> sinkAttributes = new HashMap<>();
      sinkAttributes.put("sink", "opc-ua-sink");
      sinkAttributes.put("node-url", nodeUrl);
      sinkAttributes.put("security-policy", "None");
      sinkAttributes.put("with-quality", "true");
      sinkAttributes.put("value-name", "value");
      sinkAttributes.put("quality-name", "quality");
      sinkAttributes.put("timeout-seconds", "5");

      final Map<String, String> sourceAttributes = new HashMap<>();
      sourceAttributes.put("user", "root");
      sourceAttributes.put("source.history.enable", "false");
      sourceAttributes.put("source.realtime.mode", "forced-log");

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .createPipe(
                  new TCreatePipeReq("testPipe", sinkAttributes)
                      .setExtractorAttributes(sourceAttributes)
                      .setProcessorAttributes(Collections.emptyMap()))
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      opcUaClient = getOpcUaClient(nodeUrl, SecurityPolicy.None);
      waitUntilRealtimePipeIsReady(opcUaClient);

      TestUtils.executeNonQuery(
          env,
          "insert into root.db.ext(time, value, quality, other) values (1, 42.5, false, 7)",
          null);

      assertEventuallyValue(
          opcUaClient,
          new NodeId(2, "root/db/ext"),
          new Variant(42.5),
          StatusCode.BAD,
          new DateTime(timestampToUtc(1)));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.dropPipe("testPipe").getCode());
    } finally {
      if (opcUaClient != null) {
        opcUaClient.disconnect().get();
      }
      if (externalServer != null) {
        externalServer.close();
      }
      if (tcpPort >= 0) {
        final String lockPath = EnvUtils.getLockFilePath(tcpPort);
        if (!new File(lockPath).delete()) {
          System.out.printf("Delete lock file %s failed%n", lockPath);
        }
      }
    }
  }

  private void waitUntilRealtimePipeIsReady(final OpcUaClient client) throws Exception {
    Throwable lastFailure = null;
    final long startTime = System.currentTimeMillis();
    long readyTime = 1;
    while (System.currentTimeMillis() - startTime <= 10_000L) {
      try {
        TestUtils.executeNonQuery(
            env,
            String.format(
                "insert into root.db.ready(time, value, quality) values (%d, %d, true)",
                readyTime, readyTime),
            null);
        final DataValue value =
            client.readValue(0, TimestampsToReturn.Both, new NodeId(2, "root/db/ready")).get();
        if (value.getValue().getValue() instanceof Double
            && (Double) value.getValue().getValue() > 0
            && StatusCode.GOOD.equals(value.getStatusCode())) {
          return;
        }
      } catch (final Throwable t) {
        lastFailure = t;
      }
      Thread.sleep(200L);
      ++readyTime;
    }

    if (lastFailure instanceof Exception) {
      throw (Exception) lastFailure;
    }
    if (lastFailure instanceof Error) {
      throw (Error) lastFailure;
    }
    throw new AssertionError("Timed out waiting for realtime OPC UA pipe readiness");
  }

  private static void assertEventuallyValue(
      final OpcUaClient client,
      final NodeId nodeId,
      final Variant expectedValue,
      final StatusCode expectedStatus,
      final DateTime expectedSourceTime)
      throws Exception {
    Throwable lastFailure = null;
    final long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime <= 10_000L) {
      try {
        final DataValue value = client.readValue(0, TimestampsToReturn.Both, nodeId).get();
        Assert.assertEquals(expectedValue, value.getValue());
        Assert.assertEquals(expectedStatus, value.getStatusCode());
        Assert.assertEquals(expectedSourceTime, value.getSourceTime());
        return;
      } catch (final Throwable t) {
        lastFailure = t;
        Thread.sleep(200L);
      }
    }

    if (lastFailure instanceof Exception) {
      throw (Exception) lastFailure;
    }
    if (lastFailure instanceof Error) {
      throw (Error) lastFailure;
    }
    throw new AssertionError("Timed out waiting for OPC UA value " + nodeId);
  }

  private static OpcUaClient getOpcUaClient(final String nodeUrl, final SecurityPolicy policy) {
    final IdentityProvider provider = new AnonymousProvider();
    final String securityDir =
        CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE
            + File.separatorChar
            + UUID.nameUUIDFromBytes(nodeUrl.getBytes(TSFileConfig.STRING_CHARSET));

    final IoTDBOpcUaClient client = new IoTDBOpcUaClient(nodeUrl, policy, provider, false);
    new ClientRunner(client, securityDir, "root", null, 10).run();
    return client.getClient();
  }

  private static final class ExternalAddNodesOpcUaServer implements AutoCloseable {
    private final ExternalAddNodesNameSpace nameSpace;

    private ExternalAddNodesOpcUaServer(final ExternalAddNodesNameSpace nameSpace) {
      this.nameSpace = nameSpace;
    }

    private static ExternalAddNodesOpcUaServer start(final int tcpPort, final int httpsPort)
        throws Exception {
      final String securityDir =
          "target"
              + File.separatorChar
              + "opc-ua-external-server-it"
              + File.separatorChar
              + UUID.randomUUID();
      final OpcUaServerBuilder builder =
          new OpcUaServerBuilder()
              .setTcpBindPort(tcpPort)
              .setHttpsBindPort(httpsPort)
              .setUser("root")
              .setPassword("root")
              .setSecurityDir(securityDir)
              .setEnableAnonymousAccess(true)
              .setSecurityPolicies(Set.of(SecurityPolicy.None))
              .setDebounceTimeMs(1);
      final OpcUaServer server = builder.build();
      final ExternalAddNodesNameSpace nameSpace = new ExternalAddNodesNameSpace(server, builder);
      nameSpace.startup();
      server.startup().get();
      return new ExternalAddNodesOpcUaServer(nameSpace);
    }

    @Override
    public void close() {
      nameSpace.shutdown();
    }
  }

  private static final class ExternalAddNodesNameSpace extends OpcUaNameSpace {

    private ExternalAddNodesNameSpace(final OpcUaServer server, final OpcUaServerBuilder builder) {
      super(server, builder);
    }

    @Override
    public synchronized void addNodes(
        final AddNodesContext context, final List<AddNodesItem> nodesToAdd) {
      final List<AddNodesResult> results = new ArrayList<>(nodesToAdd.size());
      for (final AddNodesItem item : nodesToAdd) {
        final AddNodesResult result = addNode(item);
        results.add(result);
      }
      context.success(results);
    }

    private AddNodesResult addNode(final AddNodesItem item) {
      final ExtensionObject attributes = item.getNodeAttributes();
      if (attributes == null) {
        return new AddNodesResult(
            new StatusCode(StatusCodes.Bad_NodeAttributesInvalid), NodeId.NULL_VALUE);
      }

      final Optional<NodeId> nodeId =
          item.getRequestedNewNodeId().toNodeId(getServer().getNamespaceTable());
      if (!nodeId.isPresent()) {
        return new AddNodesResult(
            new StatusCode(StatusCodes.Bad_NodeIdRejected), NodeId.NULL_VALUE);
      }
      if (getNodeManager().containsNode(nodeId.get())) {
        return new AddNodesResult(new StatusCode(StatusCodes.Bad_NodeIdExists), NodeId.NULL_VALUE);
      }

      final Optional<NodeId> parentId =
          item.getParentNodeId().toNodeId(getServer().getNamespaceTable());
      if (!parentId.isPresent()) {
        return new AddNodesResult(
            new StatusCode(StatusCodes.Bad_ParentNodeIdInvalid), NodeId.NULL_VALUE);
      }
      final Optional<UaNode> parentNode =
          getServer().getAddressSpaceManager().getManagedNode(parentId.get());
      if (!parentNode.isPresent()) {
        return new AddNodesResult(
            new StatusCode(StatusCodes.Bad_ParentNodeIdInvalid), NodeId.NULL_VALUE);
      }

      final Optional<NodeId> typeDefinition =
          item.getTypeDefinition().toNodeId(getServer().getNamespaceTable());
      if (!typeDefinition.isPresent()) {
        return new AddNodesResult(
            new StatusCode(StatusCodes.Bad_TypeDefinitionInvalid), NodeId.NULL_VALUE);
      }

      final UaNode newNode;
      switch (item.getNodeClass()) {
        case Variable:
          final VariableAttributes variableAttributes =
              (VariableAttributes) attributes.decode(getServer().getSerializationContext());
          newNode =
              new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
                  .setNodeId(nodeId.get())
                  .setBrowseName(item.getBrowseName())
                  .setDisplayName(variableAttributes.getDisplayName())
                  .setDataType(variableAttributes.getDataType())
                  .setTypeDefinition(typeDefinition.get())
                  .setValue(
                      new DataValue(
                          variableAttributes.getValue(),
                          StatusCode.GOOD,
                          new DateTime(0),
                          new DateTime()))
                  .setAccessLevel(variableAttributes.getAccessLevel())
                  .setUserAccessLevel(variableAttributes.getUserAccessLevel())
                  .build();
          break;
        case Object:
          final ObjectAttributes objectAttributes =
              (ObjectAttributes) attributes.decode(getServer().getSerializationContext());
          if (!Identifiers.FolderType.equals(typeDefinition.get())) {
            return new AddNodesResult(
                new StatusCode(StatusCodes.Bad_TypeDefinitionInvalid), NodeId.NULL_VALUE);
          }
          newNode =
              new UaFolderNode(
                  getNodeContext(),
                  nodeId.get(),
                  item.getBrowseName(),
                  objectAttributes.getDisplayName());
          break;
        default:
          return new AddNodesResult(
              new StatusCode(StatusCodes.Bad_NodeClassInvalid), NodeId.NULL_VALUE);
      }

      getNodeManager().addNode(newNode);
      parentNode
          .get()
          .addReference(
              new Reference(
                  parentNode.get().getNodeId(),
                  item.getReferenceTypeId(),
                  newNode.getNodeId().expanded(),
                  true));
      return new AddNodesResult(StatusCode.GOOD, newNode.getNodeId());
    }
  }
}
