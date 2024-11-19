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

package org.apache.iotdb.db.pipe.connector.protocol.opcua;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.db.pipe.connector.util.PipeTabletEventSorter;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.core.Reference;
import org.eclipse.milo.opcua.sdk.server.Lifecycle;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.api.DataItem;
import org.eclipse.milo.opcua.sdk.server.api.ManagedNamespaceWithLifecycle;
import org.eclipse.milo.opcua.sdk.server.api.MonitoredItem;
import org.eclipse.milo.opcua.sdk.server.model.nodes.objects.BaseEventTypeNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaFolderNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.sdk.server.util.SubscriptionModel;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;

import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class OpcUaNameSpace extends ManagedNamespaceWithLifecycle {
  public static final String NAMESPACE_URI = "urn:apache:iotdb:opc-server";
  private final boolean isClientServerModel;
  private final SubscriptionModel subscriptionModel;
  private final OpcUaServerBuilder builder;

  OpcUaNameSpace(
      final OpcUaServer server,
      final boolean isClientServerModel,
      final OpcUaServerBuilder builder) {
    super(server, NAMESPACE_URI);
    this.isClientServerModel = isClientServerModel;
    this.builder = builder;

    subscriptionModel = new SubscriptionModel(server, this);
    getLifecycleManager().addLifecycle(subscriptionModel);
    getLifecycleManager()
        .addLifecycle(
            new Lifecycle() {
              @Override
              public void startup() {
                // Do nothing
              }

              @Override
              public void shutdown() {
                getServer().shutdown();
                builder.close();
              }
            });
  }

  void transfer(final Tablet tablet) throws UaException {
    if (isClientServerModel) {
      transferTabletForClientServerModel(tablet);
    } else {
      transferTabletForPubSubModel(tablet);
    }
  }

  private void transferTabletForClientServerModel(final Tablet tablet) {
    new PipeTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    final String[] segments = tablet.getDeviceId().split("\\.");
    if (segments.length == 0) {
      throw new PipeRuntimeCriticalException("The segments of tablets must exist");
    }
    final StringBuilder currentStr = new StringBuilder();
    UaFolderNode folderNode = null;
    NodeId folderNodeId;
    for (final String segment : segments) {
      final UaFolderNode nextFolderNode;

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
          folderNode.addOrganizes(nextFolderNode);
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
            (UaFolderNode)
                getNodeManager()
                    .getNode(folderNodeId)
                    .orElseThrow(
                        () ->
                            new PipeRuntimeCriticalException(
                                String.format(
                                    "The folder node for %s does not exist.",
                                    tablet.getDeviceId())));
      }
    }

    final String currentFolder = currentStr.toString();
    for (int i = 0; i < tablet.getSchemas().size(); ++i) {
      final IMeasurementSchema measurementSchema = tablet.getSchemas().get(i);
      final String name = measurementSchema.getMeasurementName();
      final TSDataType type = measurementSchema.getType();
      final NodeId nodeId = newNodeId(currentFolder + name);
      final UaVariableNode measurementNode;
      if (!getNodeManager().containsNode(nodeId)) {
        measurementNode =
            new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
                .setNodeId(newNodeId(currentFolder + name))
                .setAccessLevel(AccessLevel.READ_WRITE)
                .setUserAccessLevel(AccessLevel.READ_ONLY)
                .setBrowseName(newQualifiedName(name))
                .setDisplayName(LocalizedText.english(name))
                .setDataType(convertToOpcDataType(type))
                .setTypeDefinition(Identifiers.BaseDataVariableType)
                .build();
        getNodeManager().addNode(measurementNode);
        folderNode.addOrganizes(measurementNode);
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

      int lastNonnullIndex = -1;
      for (int j = 0; j < tablet.getRowSize(); ++j) {
        if (!tablet.bitMaps[i].isMarked(j)) {
          lastNonnullIndex = j;
          break;
        }
      }

      if (lastNonnullIndex != -1) {
        final long utcTimestamp = timestampToUtc(tablet.timestamps[lastNonnullIndex]);
        if (Objects.isNull(measurementNode.getValue())
            || Objects.requireNonNull(measurementNode.getValue().getSourceTime()).getUtcTime()
                < utcTimestamp) {
          measurementNode.setValue(
              new DataValue(
                  new Variant(getTabletObjectValue4Opc(tablet.values[i], lastNonnullIndex, type)),
                  StatusCode.GOOD,
                  new DateTime(utcTimestamp),
                  new DateTime()));
        }
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

  private static long timestampToUtc(final long timeStamp) {
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
          tablet.getDeviceId()
              + TsFileConstant.PATH_SEPARATOR
              + tablet.getSchemas().get(columnIndex).getMeasurementName());

      // Source node --> Sensor type, like double
      eventNode.setSourceNode(convertToOpcDataType(dataType));

      for (int rowIndex = 0; rowIndex < tablet.getRowSize(); ++rowIndex) {
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
                    (((LocalDate[]) tablet.values[columnIndex])[rowIndex])
                        .atStartOfDay(ZoneId.systemDefault())
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

  private NodeId convertToOpcDataType(final TSDataType type) {
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

  @Override
  public void onDataItemsCreated(final List<DataItem> dataItems) {
    subscriptionModel.onDataItemsCreated(dataItems);
  }

  @Override
  public void onDataItemsModified(final List<DataItem> dataItems) {
    subscriptionModel.onDataItemsModified(dataItems);
  }

  @Override
  public void onDataItemsDeleted(final List<DataItem> dataItems) {
    subscriptionModel.onDataItemsDeleted(dataItems);
  }

  @Override
  public void onMonitoringModeChanged(final List<MonitoredItem> monitoredItems) {
    subscriptionModel.onMonitoringModeChanged(monitoredItems);
  }

  /////////////////////////////// Conflict detection ///////////////////////////////

  void checkEquals(
      final String user,
      final String password,
      final String securityDir,
      final boolean enableAnonymousAccess) {
    builder.checkEquals(user, password, Paths.get(securityDir), enableAnonymousAccess);
  }
}
