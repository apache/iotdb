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

import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.utils.TestOnly;
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
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExtensionObject;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesItem;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesResponse;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesResult;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.ObjectAttributes;
import org.eclipse.milo.opcua.stack.core.types.structured.VariableAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace.convertToOpcDataType;
import static org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace.timestampToUtc;

public class IoTDBOpcUaClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpcUaNameSpace.class);

  // Customized nodes
  private static final int NAME_SPACE_INDEX = 2;

  // Useless for a server only accept client writing
  private static final double SAMPLING_INTERVAL_PLACEHOLDER = 500;
  private final String nodeUrl;

  private final SecurityPolicy securityPolicy;
  private final IdentityProvider identityProvider;
  private OpcUaClient client;
  private final boolean historizing;

  public IoTDBOpcUaClient(
      final String nodeUrl,
      final SecurityPolicy securityPolicy,
      final IdentityProvider identityProvider,
      final boolean historizing) {
    this.nodeUrl = nodeUrl;
    this.securityPolicy = securityPolicy;
    this.identityProvider = identityProvider;
    this.historizing = historizing;
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
    StatusCode currentQuality = sink.getDefaultQuality();
    Object value = null;
    long timestamp = 0;
    NodeId nodeId = null;
    NodeId opcDataType = null;

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
        PipeLogger.log(
            LOGGER::warn,
            "When the 'with-quality' mode is enabled, the measurement must be either \"value-name\" or \"quality-name\"");
        continue;
      }
      nodeId = new NodeId(NAME_SPACE_INDEX, String.join("/", segments));

      final long utcTimestamp = timestampToUtc(timestamps.get(timestamps.size() > 1 ? i : 0));
      value = values.get(i);
      timestamp = utcTimestamp;
      opcDataType = convertToOpcDataType(type);
    }
    if (Objects.isNull(value)) {
      return;
    }

    final Variant variant = new Variant(value);
    final DataValue dataValue =
        new DataValue(variant, currentQuality, new DateTime(timestamp), new DateTime());
    StatusCode writeStatus = client.writeValue(nodeId, dataValue).get();

    if (writeStatus.getValue() == StatusCodes.Bad_NodeIdUnknown) {
      final AddNodesResponse addStatus =
          client.addNodes(getNodesToAdd(segments, opcDataType, variant)).get();
      for (final AddNodesResult result : addStatus.getResults()) {
        if (!result.getStatusCode().equals(StatusCode.GOOD)
            && !(result.getStatusCode().getValue() == StatusCodes.Bad_NodeIdExists)) {
          throw new PipeException(
              "Failed to create nodes after transfer data value, creation status: "
                  + addStatus
                  + getErrorString(segments, opcDataType, value, writeStatus));
        }
      }
      writeStatus = client.writeValue(nodeId, dataValue).get();
      if (writeStatus.getValue() != StatusCode.GOOD.getValue()) {
        throw new PipeException(
            "Failed to transfer dataValue after successfully created nodes"
                + getErrorString(segments, opcDataType, value, writeStatus));
      }
    } else if (writeStatus.getValue() != StatusCode.GOOD.getValue()) {
      throw new PipeException(
          "Failed to transfer dataValue"
              + getErrorString(segments, opcDataType, value, writeStatus));
    }
  }

  private static String getErrorString(
      final String[] segments,
      final NodeId dataType,
      final Object value,
      final StatusCode writeStatus) {
    return ", segments: "
        + Arrays.toString(segments)
        + ", dataType: "
        + dataType
        + ", value: "
        + value
        + ", error: "
        + writeStatus;
  }

  public List<AddNodesItem> getNodesToAdd(
      final String[] segments, final NodeId opcDataType, final Variant initialValue) {
    final List<AddNodesItem> addNodesItems = new ArrayList<>();
    final StringBuilder sb = new StringBuilder(segments[0]);
    ExpandedNodeId curNodeId = new NodeId(NAME_SPACE_INDEX, segments[0]).expanded();
    addNodesItems.add(
        new AddNodesItem(
            Identifiers.ObjectsFolder.expanded(),
            Identifiers.Organizes,
            curNodeId,
            new QualifiedName(NAME_SPACE_INDEX, segments[0]),
            NodeClass.Object,
            ExtensionObject.encode(
                client.getStaticSerializationContext(), createFolderAttributes(segments[0])),
            Identifiers.FolderType.expanded()));

    // segments.length >= 3
    for (int i = 1; i < segments.length - 1; ++i) {
      sb.append("/").append(segments[i]);
      final ExpandedNodeId nextId = new NodeId(NAME_SPACE_INDEX, sb.toString()).expanded();
      addNodesItems.add(
          new AddNodesItem(
              curNodeId,
              Identifiers.Organizes,
              nextId,
              new QualifiedName(NAME_SPACE_INDEX, segments[i]),
              NodeClass.Object,
              ExtensionObject.encode(
                  client.getStaticSerializationContext(), createFolderAttributes(segments[i])),
              Identifiers.FolderType.expanded()));
      curNodeId = nextId;
    }

    final String measurementName = segments[segments.length - 1];
    sb.append("/").append(measurementName);
    addNodesItems.add(
        new AddNodesItem(
            curNodeId,
            Identifiers.Organizes,
            new NodeId(NAME_SPACE_INDEX, sb.toString()).expanded(),
            new QualifiedName(NAME_SPACE_INDEX, measurementName),
            NodeClass.Variable,
            ExtensionObject.encode(
                client.getStaticSerializationContext(),
                createMeasurementAttributes(measurementName, opcDataType, initialValue)),
            Identifiers.BaseDataVariableType.expanded()));

    return addNodesItems;
  }

  public void disconnect() throws Exception {
    client.disconnect().get();
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

  @TestOnly
  public OpcUaClient getClient() {
    return client;
  }

  /////////////////////////////// Attribute creator ///////////////////////////////

  private VariableAttributes createMeasurementAttributes(
      final String name, final NodeId objectType, final Variant initialValue) {
    return new VariableAttributes(
        Unsigned.uint(0xFFFF), // specifiedAttributes
        LocalizedText.english(name),
        LocalizedText.english(name),
        Unsigned.uint(0), // writeMask
        Unsigned.uint(0), // userWriteMask
        initialValue,
        objectType,
        ValueRanks.Scalar,
        null, // arrayDimensions
        AccessLevel.toValue(AccessLevel.READ_WRITE),
        AccessLevel.toValue(AccessLevel.READ_WRITE),
        SAMPLING_INTERVAL_PLACEHOLDER,
        historizing);
  }

  private static ObjectAttributes createFolderAttributes(final String name) {
    return new ObjectAttributes(
        Unsigned.uint(0xFFFF), // specifiedAttributes
        LocalizedText.english(name),
        LocalizedText.english(name),
        Unsigned.uint(0), // writeMask
        Unsigned.uint(0), // userWriteMask
        null // notifier
        );
  }
}
