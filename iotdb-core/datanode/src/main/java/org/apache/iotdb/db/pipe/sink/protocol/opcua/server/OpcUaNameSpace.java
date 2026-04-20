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

package org.apache.iotdb.db.pipe.sink.protocol.opcua.server;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.OpcUaSink;
import org.apache.iotdb.db.pipe.sink.util.PipeTabletEventSorter;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.core.Reference;
import org.eclipse.milo.opcua.sdk.server.Lifecycle;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.api.DataItem;
import org.eclipse.milo.opcua.sdk.server.api.ManagedNamespaceWithLifecycle;
import org.eclipse.milo.opcua.sdk.server.api.MonitoredItem;
import org.eclipse.milo.opcua.sdk.server.model.nodes.objects.BaseEventTypeNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaFolderNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class OpcUaNameSpace extends ManagedNamespaceWithLifecycle {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpcUaNameSpace.class);
  public static final String NAMESPACE_URI = "urn:apache:iotdb:opc-server";
  private final OpcUaServerBuilder builder;

  // Do not use subscription model because the original subscription model has some bugs
  private final ConcurrentMap<NodeId, List<DataItem>> nodeSubscriptions = new ConcurrentHashMap<>();

  // Debounce task cache: used to merge updates within a short period of time, avoiding unnecessary
  // duplicate pushes
  private final ConcurrentMap<NodeId, ScheduledFuture<?>> debounceTasks = new ConcurrentHashMap<>();
  // Debounce interval: within 10ms, the same node is updated multiple times, and only the last one
  // will be pushed (can be adjusted according to your site delay requirements, the minimum can be
  // set to 1ms)
  private final long debounceIntervalMs;

  public OpcUaNameSpace(final OpcUaServer server, final OpcUaServerBuilder builder) {
    super(server, NAMESPACE_URI);
    this.builder = builder;
    debounceIntervalMs = builder.getDebounceTimeMs();

    getLifecycleManager()
        .addLifecycle(
            new Lifecycle() {
              @Override
              public void startup() {
                // Do nothing
              }

              @Override
              public void shutdown() {
                try {
                  getServer().shutdown();
                } finally {
                  builder.close();
                }
              }
            });
  }

  public void transfer(final Tablet tablet, final OpcUaSink sink) throws Exception {
    if (sink.isClientServerModel()) {
      transferTabletForClientServerModel(tablet, sink, this::transferTabletRowForClientServerModel);
    } else {
      transferTabletForPubSubModel(tablet);
    }
  }

  public static void transferTabletForClientServerModel(
      final Tablet tablet, final OpcUaSink sink, final TabletRowConsumer consumer)
      throws Exception {
    final List<MeasurementSchema> schemas = tablet.getSchemas();
    final List<MeasurementSchema> newSchemas = new ArrayList<>();
    new PipeTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    final List<Long> timestamps = new ArrayList<>();
    final List<Object> values = new ArrayList<>();

    for (int i = 0; i < schemas.size(); ++i) {
      for (int j = tablet.rowSize - 1; j >= 0; --j) {
        if (tablet.bitMaps == null || tablet.bitMaps[i] == null || !tablet.bitMaps[i].isMarked(j)) {
          newSchemas.add(schemas.get(i));
          timestamps.add(tablet.timestamps[j]);
          values.add(getTabletObjectValue4Opc(tablet.values[i], j, schemas.get(i).getType()));
          break;
        }
      }
    }

    consumer.accept(tablet.deviceId.split("\\."), newSchemas, timestamps, values, sink);
  }

  @FunctionalInterface
  public interface TabletRowConsumer {
    void accept(
        final String[] segments,
        final List<MeasurementSchema> measurementSchemas,
        final List<Long> timestamps,
        final List<Object> values,
        final OpcUaSink sink)
        throws Exception;
  }

  private void transferTabletRowForClientServerModel(
      final String[] segments,
      final List<MeasurementSchema> measurementSchemas,
      final List<Long> timestamps,
      final List<Object> values,
      final OpcUaSink sink) {
    if (segments.length == 0) {
      throw new PipeRuntimeCriticalException("The segments of tablets must exist");
    }
    final StringBuilder currentStr = new StringBuilder();
    UaNode folderNode = null;
    NodeId folderNodeId;
    for (int i = 0;
        i < (Objects.isNull(sink.getValueName()) ? segments.length : segments.length - 1);
        ++i) {
      final String segment = segments[i];
      final UaNode nextFolderNode;

      currentStr.append(segment);
      folderNodeId = newNodeId(currentStr.toString());
      currentStr.append("/");

      if (!getNodeManager().containsNode(folderNodeId)) {
        nextFolderNode =
            new UaFolderNode(
                getNodeContext(),
                folderNodeId,
                newQualifiedName(segment),
                LocalizedText.english(segment));
        getNodeManager().addNode(nextFolderNode);
        if (Objects.nonNull(folderNode)) {
          folderNode.addReference(
              new Reference(
                  folderNode.getNodeId(),
                  Identifiers.Organizes,
                  nextFolderNode.getNodeId().expanded(),
                  true));
        } else {
          nextFolderNode.addReference(
              new Reference(
                  folderNodeId,
                  Identifiers.Organizes,
                  Identifiers.ObjectsFolder.expanded(),
                  false));
        }
        folderNode = nextFolderNode;
      } else {
        folderNode =
            getNodeManager()
                .getNode(folderNodeId)
                .orElseThrow(
                    () ->
                        new PipeRuntimeCriticalException(
                            String.format(
                                "The folder node for %s does not exist.",
                                Arrays.toString(segments))));
      }
    }

    final String currentFolder = currentStr.toString();

    StatusCode currentQuality = sink.getDefaultQuality();
    UaVariableNode valueNode = null;
    Object value = null;
    long timestamp = 0;

    for (int i = 0; i < measurementSchemas.size(); ++i) {
      if (Objects.isNull(values.get(i))) {
        continue;
      }
      final String name = measurementSchemas.get(i).getMeasurementId();
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
      final String nodeName =
          Objects.isNull(sink.getValueName()) ? name : segments[segments.length - 1];
      final NodeId nodeId = newNodeId(currentFolder + nodeName);
      final UaVariableNode measurementNode;
      final long utcTimestamp = timestampToUtc(timestamps.get(timestamps.size() > 1 ? i : 0));
      final DataValue dataValue =
          new DataValue(
              new Variant(values.get(i)),
              currentQuality,
              new DateTime(utcTimestamp),
              new DateTime());

      if (!getNodeManager().containsNode(nodeId)) {
        measurementNode =
            new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
                .setNodeId(nodeId)
                .setAccessLevel(AccessLevel.READ_ONLY)
                .setUserAccessLevel(AccessLevel.READ_ONLY)
                .setBrowseName(newQualifiedName(nodeName))
                .setDisplayName(LocalizedText.english(nodeName))
                .setDataType(convertToOpcDataType(type))
                .setTypeDefinition(Identifiers.BaseDataVariableType)
                .setValue(dataValue)
                .build();
        getNodeManager().addNode(measurementNode);
        if (Objects.nonNull(folderNode)) {
          folderNode.addReference(
              new Reference(
                  folderNode.getNodeId(), Identifiers.Organizes, nodeId.expanded(), true));
        } else {
          measurementNode.addReference(
              new Reference(
                  nodeId, Identifiers.Organizes, Identifiers.ObjectsFolder.expanded(), false));
        }
      } else {
        // This must exist
        measurementNode =
            (UaVariableNode)
                getNodeManager()
                    .getNode(nodeId)
                    .orElseThrow(
                        () ->
                            new PipeRuntimeCriticalException(
                                String.format("The Node %s does not exist.", nodeId)));
      }

      if (Objects.isNull(sink.getValueName())) {
        if (Objects.isNull(measurementNode.getValue())
            || Objects.isNull(measurementNode.getValue().getSourceTime())
            || measurementNode.getValue().getSourceTime().getUtcTime() < utcTimestamp) {
          notifyNodeValueChange(nodeId, dataValue, measurementNode);
        }
      } else {
        valueNode = measurementNode;
        value = values.get(i);
        timestamp = utcTimestamp;
      }
    }
    if (Objects.nonNull(valueNode)) {
      if (Objects.isNull(valueNode.getValue())
          || Objects.isNull(valueNode.getValue().getSourceTime())
          || valueNode.getValue().getSourceTime().getUtcTime() < timestamp) {
        notifyNodeValueChange(
            valueNode.getNodeId(),
            new DataValue(
                new Variant(value), currentQuality, new DateTime(timestamp), new DateTime()),
            valueNode);
      }
    }
  }

  private static Object getTabletObjectValue4Opc(
      final Object column, final int rowIndex, final TSDataType type) {
    switch (type) {
      case BOOLEAN:
        return ((boolean[]) column)[rowIndex];
      case INT32:
        return ((int[]) column)[rowIndex];
      case DATE:
        return new DateTime(Date.valueOf(((LocalDate[]) column)[rowIndex]));
      case INT64:
        return ((long[]) column)[rowIndex];
      case TIMESTAMP:
        return new DateTime(timestampToUtc(((long[]) column)[rowIndex]));
      case FLOAT:
        return ((float[]) column)[rowIndex];
      case DOUBLE:
        return ((double[]) column)[rowIndex];
      case TEXT:
      case BLOB:
      case STRING:
        return ((Binary[]) column)[rowIndex].toString();
      default:
        throw new UnSupportedDataTypeException("UnSupported dataType " + type);
    }
  }

  public static long timestampToUtc(final long timeStamp) {
    return TimestampPrecisionUtils.currPrecision.toNanos(timeStamp) / 100L + 116444736000000000L;
  }

  /**
   * Transfer {@link Tablet} into eventNodes and post it on the eventBus, so that they will be heard
   * at the subscribers. Notice that an eventNode is reused to reduce object creation costs.
   *
   * @param tablet the tablet to send
   * @throws UaException if failed to create {@link Event}
   */
  private void transferTabletForPubSubModel(final Tablet tablet) throws UaException {
    final BaseEventTypeNode eventNode =
        getServer()
            .getEventFactory()
            .createEvent(
                new NodeId(getNamespaceIndex(), UUID.randomUUID()), Identifiers.BaseEventType);

    // Use eventNode here because other nodes doesn't support values and times simultaneously
    for (int columnIndex = 0; columnIndex < tablet.getSchemas().size(); ++columnIndex) {
      final TSDataType dataType = tablet.getSchemas().get(columnIndex).getType();

      // Source name --> Sensor path, like root.test.d_0.s_0
      eventNode.setSourceName(
          tablet.deviceId
              + TsFileConstant.PATH_SEPARATOR
              + tablet.getSchemas().get(columnIndex).getMeasurementId());

      // Source node --> Sensor type, like double
      eventNode.setSourceNode(convertToOpcDataType(dataType));

      for (int rowIndex = 0; rowIndex < tablet.rowSize; ++rowIndex) {
        // Filter null value
        if (tablet.bitMaps[columnIndex].isMarked(rowIndex)) {
          continue;
        }

        // Time --> TimeStamp
        eventNode.setTime(new DateTime(timestampToUtc(tablet.timestamps[rowIndex])));

        // Message --> Value
        switch (dataType) {
          case BOOLEAN:
            eventNode.setMessage(
                LocalizedText.english(
                    Boolean.toString(((boolean[]) tablet.values[columnIndex])[rowIndex])));
            break;
          case INT32:
            eventNode.setMessage(
                LocalizedText.english(
                    Integer.toString(((int[]) tablet.values[columnIndex])[rowIndex])));
            break;
          case DATE:
            eventNode.setMessage(
                LocalizedText.english(
                    ((LocalDate[]) tablet.values[columnIndex])
                        [rowIndex].atStartOfDay(ZoneId.systemDefault())
                        .toString()));
            break;
          case INT64:
            eventNode.setMessage(
                LocalizedText.english(
                    Long.toString(((long[]) tablet.values[columnIndex])[rowIndex])));
            break;
          case TIMESTAMP:
            eventNode.setMessage(
                LocalizedText.english(
                    DateTimeUtils.convertLongToDate(
                        ((long[]) tablet.values[columnIndex])[rowIndex])));
            break;
          case FLOAT:
            eventNode.setMessage(
                LocalizedText.english(
                    Float.toString(((float[]) tablet.values[columnIndex])[rowIndex])));
            break;
          case DOUBLE:
            eventNode.setMessage(
                LocalizedText.english(
                    Double.toString(((double[]) tablet.values[columnIndex])[rowIndex])));
            break;
          case TEXT:
          case BLOB:
          case STRING:
            eventNode.setMessage(
                LocalizedText.english(
                    ((Binary[]) tablet.values[columnIndex])[rowIndex].toString()));
            break;
          case VECTOR:
          case UNKNOWN:
          default:
            throw new PipeRuntimeNonCriticalException(
                "Unsupported data type: " + tablet.getSchemas().get(columnIndex).getType());
        }

        // Send the event
        getServer().getEventBus().post(eventNode);
      }
    }
    eventNode.delete();
  }

  public static NodeId convertToOpcDataType(final TSDataType type) {
    switch (type) {
      case BOOLEAN:
        return Identifiers.Boolean;
      case INT32:
        return Identifiers.Int32;
      case DATE:
      case TIMESTAMP:
        return Identifiers.DateTime;
      case INT64:
        return Identifiers.Int64;
      case FLOAT:
        return Identifiers.Float;
      case DOUBLE:
        return Identifiers.Double;
      case TEXT:
      case BLOB:
      case STRING:
        return Identifiers.String;
      case VECTOR:
      case UNKNOWN:
      default:
        throw new PipeRuntimeNonCriticalException("Unsupported data type: " + type);
    }
  }

  /**
   * On point value changing, notify all subscribed clients proactively
   *
   * @param nodeId NodeId of the changing node
   * @param newValue New value of the node (DataValue object containing value, status code, and
   *     timestamp)
   * @param variableNode Corresponding UaVariableNode instance, used to update the local cached
   *     value of the node
   */
  public void notifyNodeValueChange(
      NodeId nodeId, DataValue newValue, UaVariableNode variableNode) {
    // 1. Update the local cached value of the node
    variableNode.setValue(newValue);

    // 2. If there are no subscribers, return directly without doing any extra operations
    List<DataItem> subscribedItems = nodeSubscriptions.get(nodeId);
    if (subscribedItems == null || subscribedItems.isEmpty()) {
      return;
    }

    // 2. Debounce+Async Push: Asynchronously push the expensive push operation, while merging
    // high-frequency repeated updates
    debounceTasks.compute(
        nodeId,
        (k, oldTask) -> {
          // If there is already a pending push task, cancel it, we only need the latest value
          if (oldTask != null && !oldTask.isDone()) {
            oldTask.cancel(false);
          }

          // Submit the push task to the Milo's scheduled thread pool, delay DEBOUNCE_INTERVAL_MS
          // execution
          return getServer()
              .getScheduledExecutorService()
              .schedule(
                  () -> {
                    try {
                      // Batch push changes to all subscribers, this time-consuming operation is put
                      // into the thread pool, not blocking your data update thread
                      for (DataItem item : subscribedItems) {
                        try {
                          item.setValue(newValue);
                        } catch (Exception e) {
                          // Single client push failure does not affect other clients
                          LOGGER.warn(
                              "Failed to push value change to client, nodeId={}", nodeId, e);
                        }
                      }
                    } finally {
                      // Task execution completed, clean up the debounce cache
                      debounceTasks.remove(nodeId);
                    }
                  },
                  debounceIntervalMs,
                  TimeUnit.MILLISECONDS);
        });
  }

  @Override
  public void onDataItemsCreated(final List<DataItem> dataItems) {
    for (DataItem item : dataItems) {
      final ReadValueId readValueId = item.getReadValueId();
      // Only handle Value attribute subscription (align with the original SubscriptionModel logic,
      // ignore other attribute subscriptions)
      if (!AttributeId.Value.isEqual(readValueId.getAttributeId())) {
        continue;
      }
      final NodeId nodeId = readValueId.getNodeId();

      // 1. Add the new subscription item to the subscription mapping
      nodeSubscriptions.compute(
          nodeId,
          (k, existingList) -> {
            List<DataItem> list =
                existingList != null ? existingList : new CopyOnWriteArrayList<>();
            list.add(item);
            return list;
          });

      // 2. 【Key Optimization】Proactively push the current node's initial value when the new
      // subscription item is created
      // Eliminate Bad_WaitingForInitialData, no need to wait for any polling
      try {
        UaVariableNode node = (UaVariableNode) getNodeManager().getNode(nodeId).orElse(null);
        if (node != null && node.getValue() != null) {
          // Immediately push the current value to the new subscriber, the client will instantly be
          // able to get the initial data
          item.setValue(node.getValue());
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to send initial value to new subscription, nodeId={}", nodeId, e);
      }
    }
  }

  @Override
  public void onDataItemsModified(final List<DataItem> dataItems) {
    // Push mode, client modifies subscription parameters (e.g. sampling interval) has no effect on
    // our active push, no additional processing is needed
  }

  @Override
  public void onDataItemsDeleted(final List<DataItem> dataItems) {
    for (DataItem item : dataItems) {
      final ReadValueId readValueId = item.getReadValueId();
      if (!AttributeId.Value.isEqual(readValueId.getAttributeId())) {
        continue;
      }
      final NodeId nodeId = readValueId.getNodeId();

      // When the client cancels the subscription, remove this subscription item from the mapping
      nodeSubscriptions.computeIfPresent(
          nodeId,
          (k, existingList) -> {
            existingList.remove(item);
            // Automatically clean up the key when there are no subscribers, save memory
            return existingList.isEmpty() ? null : existingList;
          });
    }
  }

  @Override
  public void onMonitoringModeChanged(final List<MonitoredItem> monitoredItems) {
    // Push mode, monitoring mode change has no effect on active push, no additional processing is
    // needed
  }

  /////////////////////////////// Conflict detection ///////////////////////////////

  public void checkEquals(
      final String user,
      final String password,
      final String securityDir,
      final boolean enableAnonymousAccess,
      final Set<SecurityPolicy> securityPolicies,
      final long debounceTimeMs) {
    builder.checkEquals(
        user,
        password,
        Paths.get(securityDir),
        enableAnonymousAccess,
        securityPolicies,
        debounceTimeMs);
  }
}
