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

package org.apache.iotdb.db.pipe.sink.protocol.opcua.client;

import org.apache.iotdb.db.pipe.sink.protocol.opcua.OpcUaSink;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.core.ValueRanks;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExtensionObject;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesItem;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesResponse;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesResult;
import org.eclipse.milo.opcua.stack.core.types.structured.DeleteNodesItem;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.ObjectAttributes;
import org.eclipse.milo.opcua.stack.core.types.structured.VariableAttributes;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace.timestampToUtc;

public class IoTDBOpcUaClient {

  private final String nodeUrl;

  private final SecurityPolicy securityPolicy;
  private final IdentityProvider identityProvider;
  private OpcUaClient client;

  public IoTDBOpcUaClient(
      final String nodeUrl,
      final SecurityPolicy securityPolicy,
      final IdentityProvider identityProvider) {
    this.nodeUrl = nodeUrl;
    this.securityPolicy = securityPolicy;
    this.identityProvider = identityProvider;
  }

  public void run(final OpcUaClient client) throws Exception {
    // synchronous connect
    this.client = client;
    client.connect().get();
  }

  // Only support tree model & client-server
  public void transfer(final Tablet tablet, final OpcUaSink sink) throws Exception {
    OpcUaNameSpace.transferTabletForClientServerModel(
        tablet, false, sink, this::transferTabletRowForClientServerModel);
  }

  private void transferTabletRowForClientServerModel(
      final String[] segments,
      final List<IMeasurementSchema> measurementSchemas,
      final List<Long> timestamps,
      final List<Object> values,
      final OpcUaSink sink)
      throws Exception {
    StatusCode currentQuality =
        Objects.isNull(sink.getValueName()) ? StatusCode.GOOD : StatusCode.UNCERTAIN;
    Object value = null;
    long timestamp = 0;
    NodeId nodeId = null;

    for (int i = 0; i < measurementSchemas.size(); ++i) {
      if (Objects.isNull(values.get(i))) {
        continue;
      }
      final String name = measurementSchemas.get(i).getMeasurementName();
      final TSDataType type = measurementSchemas.get(i).getType();
      if (Objects.nonNull(sink.getQualityName()) && sink.getQualityName().equals(name)) {
        if (!type.equals(TSDataType.BOOLEAN)) {
          throw new UnsupportedOperationException(
              "The quality value only supports boolean type, while true == GOOD and false == BAD.");
        }
        currentQuality = values.get(i) == Boolean.TRUE ? StatusCode.GOOD : StatusCode.BAD;
        continue;
      }
      if (Objects.nonNull(sink.getValueName()) && !sink.getValueName().equals(name)) {
        throw new UnsupportedOperationException(
            "When the 'with-quality' mode is enabled, the measurement must be either \"value-name\" or \"quality-name\"");
      }
      nodeId = new NodeId(2, String.join("/", segments));

      final long utcTimestamp = timestampToUtc(timestamps.get(timestamps.size() > 1 ? i : 0));
      value = values.get(i);
      timestamp = utcTimestamp;
    }
    final DataValue dataValue =
        new DataValue(new Variant(value), currentQuality, new DateTime(timestamp), new DateTime());
    StatusCode writeStatus = client.writeValue(nodeId, dataValue).get();

    if (writeStatus.getValue() == StatusCodes.Bad_NodeIdUnknown) {
      final AddNodesResponse addStatus =
          client
              .addNodes(
                  Arrays.asList(
                      new AddNodesItem(
                          Identifiers.ObjectsFolder.expanded(),
                          Identifiers.Organizes,
                          new NodeId(2, "root").expanded(),
                          new QualifiedName(2, "root"),
                          NodeClass.Object,
                          ExtensionObject.encode(
                              client.getStaticSerializationContext(), createFolder0Attributes()),
                          Identifiers.FolderType.expanded()),
                      new AddNodesItem(
                          new NodeId(2, "root").expanded(),
                          Identifiers.Organizes,
                          new NodeId(2, "root/sg").expanded(),
                          new QualifiedName(2, "sg"),
                          NodeClass.Object,
                          ExtensionObject.encode(
                              client.getStaticSerializationContext(), createFolder1Attributes()),
                          Identifiers.FolderType.expanded()),
                      new AddNodesItem(
                          new NodeId(2, "root/sg").expanded(),
                          Identifiers.Organizes,
                          new NodeId(2, "root/sg/d1").expanded(),
                          new QualifiedName(2, "d2"),
                          NodeClass.Object,
                          ExtensionObject.encode(
                              client.getStaticSerializationContext(), createFolder2Attributes()),
                          Identifiers.FolderType.expanded()),
                      new AddNodesItem(
                          new NodeId(2, "root/sg/d1").expanded(),
                          Identifiers.Organizes,
                          new NodeId(2, "root/sg/d1/s2").expanded(),
                          new QualifiedName(2, "s2"),
                          NodeClass.Variable,
                          ExtensionObject.encode(
                              client.getStaticSerializationContext(),
                              createPressureSensorAttributes()),
                          Identifiers.BaseDataVariableType.expanded())))
              .get();
      for (final AddNodesResult result : addStatus.getResults()) {
        if (!result.getStatusCode().equals(StatusCode.GOOD)
            && !(result.getStatusCode().getValue() == StatusCodes.Bad_NodeIdExists)) {
          throw new PipeException(
              "Failed to create nodes after transfer data value, write status: "
                  + writeStatus
                  + ", creation status: "
                  + addStatus);
        }
      }
      writeStatus = client.writeValue(nodeId, dataValue).get();
      if (writeStatus.getValue() != StatusCode.GOOD.getValue()) {
        throw new PipeException(
            "Failed to transfer dataValue after successfully created nodes, error: " + writeStatus);
      }
    } else {
      throw new PipeException("Failed to transfer dataValue, error: " + writeStatus);
    }
  }

  /////////////////////////////// Getter ///////////////////////////////

  String getNodeUrl() {
    return nodeUrl;
  }

  Predicate<EndpointDescription> endpointFilter() {
    return e -> getSecurityPolicy().getUri().equals(e.getSecurityPolicyUri());
  }

  SecurityPolicy getSecurityPolicy() {
    return securityPolicy;
  }

  IdentityProvider getIdentityProvider() {
    return identityProvider;
  }

  public void runA(OpcUaClient client) throws Exception {
    // synchronous connect
    client.connect().get();
    System.out.println("✅ 连接成功");

    // 读取标签值c
    NodeId nodeId = new NodeId(2, "root/sg/d1/s2");

    // 1. 先读取当前值确认节点可访问
    DataValue readValue = client.readValue(0, TimestampsToReturn.Both, nodeId).get();
    System.out.println("读取当前值: " + readValue.getValue().getValue());
    System.out.println("读取状态: " + readValue.getStatusCode());

    // 2. 尝试写入新值
    Variant newValue = new Variant(42.0f);
    DataValue writeValue = new DataValue(newValue, StatusCode.GOOD, new DateTime(), new DateTime());

    System.out.println("尝试写入值: " + newValue.getValue());

    StatusCode writeStatus = client.writeValue(nodeId, writeValue).get();
    System.out.println("写入状态: " + writeStatus);

    client.deleteNodes(Collections.singletonList(new DeleteNodesItem(nodeId, true)));

    AddNodesResponse addStatus =
        client
            .addNodes(
                Arrays.asList(
                    new AddNodesItem(
                        Identifiers.ObjectsFolder.expanded(),
                        Identifiers.Organizes,
                        new NodeId(2, "root").expanded(),
                        new QualifiedName(2, "root"),
                        NodeClass.Object,
                        ExtensionObject.encode(
                            client.getStaticSerializationContext(), createFolder0Attributes()),
                        Identifiers.FolderType.expanded()),
                    new AddNodesItem(
                        new NodeId(2, "root").expanded(),
                        Identifiers.Organizes,
                        new NodeId(2, "root/sg").expanded(),
                        new QualifiedName(2, "sg"),
                        NodeClass.Object,
                        ExtensionObject.encode(
                            client.getStaticSerializationContext(), createFolder1Attributes()),
                        Identifiers.FolderType.expanded()),
                    new AddNodesItem(
                        new NodeId(2, "root/sg").expanded(),
                        Identifiers.Organizes,
                        new NodeId(2, "root/sg/d1").expanded(),
                        new QualifiedName(2, "d2"),
                        NodeClass.Object,
                        ExtensionObject.encode(
                            client.getStaticSerializationContext(), createFolder2Attributes()),
                        Identifiers.FolderType.expanded()),
                    new AddNodesItem(
                        new NodeId(2, "root/sg/d1").expanded(),
                        Identifiers.Organizes,
                        new NodeId(2, "root/sg/d1/s2").expanded(),
                        new QualifiedName(2, "s2"),
                        NodeClass.Variable,
                        ExtensionObject.encode(
                            client.getStaticSerializationContext(),
                            createPressureSensorAttributes()),
                        Identifiers.BaseDataVariableType.expanded())))
            .get();
    System.out.println("新增节点状态: " + addStatus);
    client.disconnect().get();
  }

  public static VariableAttributes createPressureSensorAttributes() {
    return new VariableAttributes(
        Unsigned.uint(0xFFFF), // specifiedAttributes
        LocalizedText.english("s2"),
        LocalizedText.english("反应釜压力传感器"),
        Unsigned.uint(0), // writeMask
        Unsigned.uint(0), // userWriteMask
        new Variant(101.3f), // 初始压力值 101.3 kPa
        Identifiers.Float, // 浮点数类型
        ValueRanks.Scalar, // 标量
        null, // arrayDimensions
        AccessLevel.toValue(AccessLevel.READ_WRITE),
        AccessLevel.toValue(AccessLevel.READ_WRITE),
        500.0, // 500ms 采样间隔
        false // 启用历史记录
        );
  }

  public static ObjectAttributes createFolder0Attributes() {
    return new ObjectAttributes(
        Unsigned.uint(0xFFFF), // specifiedAttributes
        LocalizedText.english("root"),
        LocalizedText.english("反应釜压力传感器"),
        Unsigned.uint(0), // writeMask
        Unsigned.uint(0), // userWriteMask
        null // 启用历史记录
        );
  }

  public static ObjectAttributes createFolder1Attributes() {
    return new ObjectAttributes(
        Unsigned.uint(0xFFFF), // specifiedAttributes
        LocalizedText.english("sg"),
        LocalizedText.english("反应釜压力传感器"),
        Unsigned.uint(0), // writeMask
        Unsigned.uint(0), // userWriteMask
        null // 启用历史记录
        );
  }

  public static ObjectAttributes createFolder2Attributes() {
    return new ObjectAttributes(
        Unsigned.uint(0xFFFF), // specifiedAttributes
        LocalizedText.english("d1"),
        LocalizedText.english("反应釜压力传感器"),
        Unsigned.uint(0), // writeMask
        Unsigned.uint(0), // userWriteMask
        null // 启用历史记录
        );
  }
}
