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
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.OpcUaSink;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.core.ValueRanks;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
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
import org.eclipse.milo.opcua.stack.core.types.structured.ObjectAttributes;
import org.eclipse.milo.opcua.stack.core.types.structured.VariableAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace.convertToOpcDataType;
import static org.apache.iotdb.db.pipe.sink.protocol.opcua.server.OpcUaNameSpace.timestampToUtc;
import static org.eclipse.milo.opcua.stack.core.StatusCodes.Bad_Timeout;
import static org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn.Neither;

public class IoTDBOpcUaClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpcUaNameSpace.class);

  private static final int DEFAULT_MAX_NODES_PER_WRITE = 10_000;
  private static final int DEFAULT_MAX_NODES_PER_NODE_MANAGEMENT = 250;

  // Customized nodes
  private static final int NAME_SPACE_INDEX = 2;

  // Useless for a server only accept client writing
  private static final double SAMPLING_INTERVAL_PLACEHOLDER = 500;
  private final String nodeUrl;

  private final SecurityPolicy securityPolicy;
  private final IdentityProvider identityProvider;
  private OpcUaClient client;
  private final boolean historizing;
  private ClientRunner runner;
  private int maxNodesPerWrite = DEFAULT_MAX_NODES_PER_WRITE;
  private int maxNodesPerNodeManagement = DEFAULT_MAX_NODES_PER_NODE_MANAGEMENT;

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
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < runner.getTimeoutSeconds() * 1000L) {
      try {
        client.connectAsync().get();
      } catch (final ExecutionException e) {
        if (e.getCause() instanceof UaException
            && ((UaException) e.getCause()).getStatusCode().getValue() == Bad_Timeout) {
          Thread.sleep(1000L);
          continue;
        }
        throw e;
      }
      break;
    }
    updateOperationLimits();
  }

  private void updateOperationLimits() {
    try {
      final List<DataValue> operationLimits =
          client
              .readValuesAsync(
                  0.0,
                  Neither,
                  Arrays.asList(
                      Identifiers.Server_ServerCapabilities_OperationLimits_MaxNodesPerWrite,
                      Identifiers
                          .Server_ServerCapabilities_OperationLimits_MaxNodesPerNodeManagement))
              .get();
      maxNodesPerWrite = getOperationLimit(operationLimits, 0, DEFAULT_MAX_NODES_PER_WRITE);
      maxNodesPerNodeManagement =
          getOperationLimit(operationLimits, 1, DEFAULT_MAX_NODES_PER_NODE_MANAGEMENT);
      LOGGER.info(
          "OPC UA server operation limits: maxNodesPerWrite={}, maxNodesPerNodeManagement={}",
          maxNodesPerWrite,
          maxNodesPerNodeManagement);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Interrupted while reading OPC UA server operation limits, use defaults: "
              + "maxNodesPerWrite={}, maxNodesPerNodeManagement={}",
          DEFAULT_MAX_NODES_PER_WRITE,
          DEFAULT_MAX_NODES_PER_NODE_MANAGEMENT);
    } catch (final Exception e) {
      LOGGER.warn(
          "Failed to read OPC UA server operation limits, use defaults: "
              + "maxNodesPerWrite={}, maxNodesPerNodeManagement={}",
          DEFAULT_MAX_NODES_PER_WRITE,
          DEFAULT_MAX_NODES_PER_NODE_MANAGEMENT,
          e);
    }
  }

  private static int getOperationLimit(
      final List<DataValue> operationLimits, final int index, final int defaultValue) {
    if (Objects.isNull(operationLimits) || operationLimits.size() <= index) {
      return defaultValue;
    }

    final DataValue dataValue = operationLimits.get(index);
    if (Objects.isNull(dataValue)
        || Objects.isNull(dataValue.getStatusCode())
        || !dataValue.getStatusCode().isGood()
        || Objects.isNull(dataValue.getValue())
        || !(dataValue.getValue().getValue() instanceof Number)) {
      return defaultValue;
    }

    final long limit = ((Number) dataValue.getValue().getValue()).longValue();
    return limit == 0 ? Integer.MAX_VALUE : (int) Math.min(limit, Integer.MAX_VALUE);
  }

  // Only support tree model & client-server
  public void transfer(final Tablet tablet, final OpcUaSink sink) throws Exception {
    OpcUaNameSpace.transferTabletForClientServerModel(
        tablet, false, sink, this::transferTabletRowForClientServerModel);
  }

  public void transferLastValues(
      final Map<IDeviceID, List<Pair<IMeasurementSchema, TimeValuePair>>> deviceLastValues,
      final boolean isTableModel,
      final OpcUaSink sink)
      throws Exception {
    final List<OpcUaWriteRequest> writeRequests = new ArrayList<>();
    for (final Map.Entry<IDeviceID, List<Pair<IMeasurementSchema, TimeValuePair>>> entry :
        deviceLastValues.entrySet()) {
      OpcUaNameSpace.transferLastValues(
          entry.getKey(),
          entry.getValue(),
          isTableModel,
          sink,
          (segments, measurementSchemas, timestamps, values, currentSink) ->
              collectWriteRequests(
                  segments, measurementSchemas, timestamps, values, currentSink, writeRequests));
    }
    writeValues(writeRequests);
  }

  private void transferTabletRowForClientServerModel(
      final String[] segments,
      final List<IMeasurementSchema> measurementSchemas,
      final List<Long> timestamps,
      final List<Object> values,
      final OpcUaSink sink)
      throws Exception {
    final List<OpcUaWriteRequest> writeRequests = new ArrayList<>();
    collectWriteRequests(segments, measurementSchemas, timestamps, values, sink, writeRequests);
    writeValues(writeRequests);
  }

  private void collectWriteRequests(
      final String[] segments,
      final List<IMeasurementSchema> measurementSchemas,
      final List<Long> timestamps,
      final List<Object> values,
      final OpcUaSink sink,
      final List<OpcUaWriteRequest> writeRequests) {
    StatusCode currentQuality = sink.getDefaultQuality();
    Object value = null;
    long timestamp = 0;
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
              DataNodePipeMessages.THE_QUALITY_VALUE_ONLY_SUPPORTS_BOOLEAN_TYPE);
        }
        currentQuality = values.get(i) == Boolean.TRUE ? StatusCode.GOOD : StatusCode.BAD;
        continue;
      }
      if (Objects.nonNull(sink.getValueName()) && !sink.getValueName().equals(name)) {
        PipeLogger.log(
            LOGGER::warn,
            DataNodePipeMessages.WITH_QUALITY_MEASUREMENT_MUST_BE_VALUE_OR_QUALITY_NAME);
        continue;
      }

      final long utcTimestamp = timestampToUtc(timestamps.get(timestamps.size() > 1 ? i : 0));
      if (Objects.isNull(sink.getValueName())) {
        writeRequests.add(
            new OpcUaWriteRequest(
                values.get(i),
                utcTimestamp,
                convertToOpcDataType(type),
                currentQuality,
                segments,
                name));
      } else {
        value = values.get(i);
        timestamp = utcTimestamp;
        opcDataType = convertToOpcDataType(type);
      }
    }
    if (Objects.nonNull(value)) {
      writeRequests.add(
          new OpcUaWriteRequest(value, timestamp, opcDataType, currentQuality, segments, null));
    }
  }

  private void writeValues(final List<OpcUaWriteRequest> writeRequests) throws Exception {
    if (writeRequests.isEmpty()) {
      return;
    }

    final List<OpcUaWriteRequest> missingNodeWriteRequests =
        getMissingNodeWriteRequests(writeRequests, writeValuesOnce(writeRequests));
    if (missingNodeWriteRequests.isEmpty()) {
      return;
    }

    addMissingNodes(missingNodeWriteRequests);
    validateRetriedWrites(missingNodeWriteRequests, writeValuesOnce(missingNodeWriteRequests));
  }

  private List<OpcUaWriteRequest> getMissingNodeWriteRequests(
      final List<OpcUaWriteRequest> writeRequests, final List<StatusCode> writeStatuses) {
    final List<OpcUaWriteRequest> missingNodeWriteRequests = new ArrayList<>();
    for (int i = 0; i < writeRequests.size(); ++i) {
      if (writeStatuses.get(i).getValue() == StatusCodes.Bad_NodeIdUnknown) {
        missingNodeWriteRequests.add(writeRequests.get(i));
      } else {
        validateInitialWrite(writeRequests.get(i), writeStatuses.get(i));
      }
    }
    return missingNodeWriteRequests;
  }

  private void validateInitialWrite(
      final OpcUaWriteRequest writeRequest, final StatusCode writeStatus) {
    if (writeStatus.getValue() != StatusCode.GOOD.getValue()) {
      throw new PipeException(
          DataNodePipeMessages.FAILED_TO_TRANSFER_DATAVALUE
              + writeRequest.getErrorString(writeStatus));
    }
  }

  private void addMissingNodes(final List<OpcUaWriteRequest> writeRequests) throws Exception {
    final List<AddNodesItem> nodesToAdd = new ArrayList<>();
    final Set<ExpandedNodeId> nodeIdsToAdd = new HashSet<>();
    for (final OpcUaWriteRequest writeRequest : writeRequests) {
      for (final AddNodesItem nodeToAdd :
          getNodesToAdd(
              writeRequest.segments,
              writeRequest.name,
              writeRequest.opcDataType,
              writeRequest.variant)) {
        if (nodeIdsToAdd.add(nodeToAdd.getRequestedNewNodeId())) {
          nodesToAdd.add(nodeToAdd);
        }
      }
    }

    for (int startIndex = 0; startIndex < nodesToAdd.size(); ) {
      final int endIndex =
          getBatchEndIndex(startIndex, nodesToAdd.size(), maxNodesPerNodeManagement);
      final AddNodesResponse addStatus =
          client.addNodesAsync(nodesToAdd.subList(startIndex, endIndex)).get();
      for (final AddNodesResult result : addStatus.getResults()) {
        if (!result.getStatusCode().equals(StatusCode.GOOD)
            && result.getStatusCode().getValue() != StatusCodes.Bad_NodeIdExists) {
          throw new PipeException(
              DataNodePipeMessages.FAILED_TO_CREATE_NODES_AFTER_TRANSFER_DATA
                  + addStatus
                  + writeRequests
                      .get(0)
                      .getErrorString(new StatusCode(StatusCodes.Bad_NodeIdUnknown)));
        }
      }
      startIndex = endIndex;
    }
  }

  private void validateRetriedWrites(
      final List<OpcUaWriteRequest> writeRequests, final List<StatusCode> writeStatuses) {
    for (int i = 0; i < writeRequests.size(); ++i) {
      if (writeStatuses.get(i).getValue() != StatusCode.GOOD.getValue()) {
        throw new PipeException(
            DataNodePipeMessages.FAILED_TO_TRANSFER_DATAVALUE_AFTER_SUCCESSFULLY_CREATED
                + writeRequests.get(i).getErrorString(writeStatuses.get(i)));
      }
    }
  }

  private List<StatusCode> writeValuesOnce(final List<OpcUaWriteRequest> writeRequests)
      throws Exception {
    final List<StatusCode> writeStatuses = new ArrayList<>(writeRequests.size());
    for (int startIndex = 0; startIndex < writeRequests.size(); ) {
      final int endIndex = getBatchEndIndex(startIndex, writeRequests.size(), maxNodesPerWrite);
      final List<NodeId> nodeIds = new ArrayList<>(endIndex - startIndex);
      final List<DataValue> dataValues = new ArrayList<>(endIndex - startIndex);
      for (int i = startIndex; i < endIndex; ++i) {
        nodeIds.add(writeRequests.get(i).nodeId);
        dataValues.add(writeRequests.get(i).dataValue);
      }
      writeStatuses.addAll(client.writeValuesAsync(nodeIds, dataValues).get());
      startIndex = endIndex;
    }
    return writeStatuses;
  }

  private static int getBatchEndIndex(
      final int startIndex, final int totalSize, final int batchSize) {
    return (int) Math.min((long) totalSize, (long) startIndex + batchSize);
  }

  private static final class OpcUaWriteRequest {
    private final Object value;
    private final NodeId opcDataType;
    private final String[] segments;
    private final @Nullable String name;
    private final NodeId nodeId;
    private final Variant variant;
    private final DataValue dataValue;

    private OpcUaWriteRequest(
        final Object value,
        final long timestamp,
        final NodeId opcDataType,
        final StatusCode currentQuality,
        final String[] segments,
        final @Nullable String name) {
      this.value = value;
      this.opcDataType = opcDataType;
      this.segments = segments;
      this.name = name;
      nodeId =
          new NodeId(
              NAME_SPACE_INDEX,
              Objects.nonNull(name)
                  ? String.join("/", segments) + "/" + name
                  : String.join("/", segments));
      variant = new Variant(value);
      dataValue = new DataValue(variant, currentQuality, new DateTime(timestamp), new DateTime());
    }

    private String getErrorString(final StatusCode writeStatus) {
      return IoTDBOpcUaClient.getErrorString(segments, name, opcDataType, value, writeStatus);
    }
  }

  private static String getErrorString(
      final String[] segments,
      final @Nullable String name,
      final NodeId dataType,
      final Object value,
      final StatusCode writeStatus) {
    return ", measurement: "
        + (Objects.nonNull(name)
            ? String.join(TsFileConstant.PATH_SEPARATOR, segments)
                + TsFileConstant.PATH_SEPARATOR
                + name
            : String.join(TsFileConstant.PATH_SEPARATOR, segments))
        + ", dataType: "
        + dataType
        + ", value: "
        + value
        + ", error: "
        + writeStatus;
  }

  public List<AddNodesItem> getNodesToAdd(
      final String[] segments,
      final @Nullable String name,
      final NodeId opcDataType,
      final Variant initialValue) {
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
                client.getStaticEncodingContext(), createFolderAttributes(segments[0])),
            Identifiers.FolderType.expanded()));

    // segments.length >= 3
    for (int i = 1; i < (Objects.nonNull(name) ? segments.length : segments.length - 1); ++i) {
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
                  client.getStaticEncodingContext(), createFolderAttributes(segments[i])),
              Identifiers.FolderType.expanded()));
      curNodeId = nextId;
    }

    final String measurementName = Objects.nonNull(name) ? name : segments[segments.length - 1];
    sb.append("/").append(measurementName);
    addNodesItems.add(
        new AddNodesItem(
            curNodeId,
            Identifiers.Organizes,
            new NodeId(NAME_SPACE_INDEX, sb.toString()).expanded(),
            new QualifiedName(NAME_SPACE_INDEX, measurementName),
            NodeClass.Variable,
            ExtensionObject.encode(
                client.getStaticEncodingContext(),
                createMeasurementAttributes(measurementName, opcDataType, initialValue)),
            Identifiers.BaseDataVariableType.expanded()));

    return addNodesItems;
  }

  public void disconnect() throws Exception {
    try {
      if (Objects.nonNull(client)) {
        client.disconnectAsync().get();
      }
    } finally {
      if (Objects.nonNull(runner)) {
        runner.close();
      }
    }
  }

  /////////////////////////////// Getter ///////////////////////////////

  String getNodeUrl() {
    return nodeUrl;
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

  /////////////////////////////// Conflict detection ///////////////////////////////

  public void setRunner(ClientRunner runner) {
    this.runner = runner;
  }

  public void checkEquals(
      final String user,
      final String password,
      final String securityDir,
      final SecurityPolicy securityPolicy,
      final boolean allowEndpointRedirect) {
    runner.checkEquals(
        user, password, Paths.get(securityDir), securityPolicy, allowEndpointRedirect);
  }
}
