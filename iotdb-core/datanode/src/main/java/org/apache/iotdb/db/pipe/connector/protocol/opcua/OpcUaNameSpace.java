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

import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.core.Reference;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.api.DataItem;
import org.eclipse.milo.opcua.sdk.server.api.ManagedNamespaceWithLifecycle;
import org.eclipse.milo.opcua.sdk.server.api.MonitoredItem;
import org.eclipse.milo.opcua.sdk.server.nodes.UaFolderNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;

import java.util.List;

public class OpcUaNameSpace extends ManagedNamespaceWithLifecycle {

  public OpcUaNameSpace(final OpcUaServer server, final String namespaceUri) {
    super(server, namespaceUri);
  }

  private void updateValues(final Tablet tablet) {
    final String[] segments = tablet.deviceId.split("\\.");
    if (segments.length == 0) {
      throw new PipeRuntimeCriticalException("The segments of tablets must exist");
    }
    final StringBuilder currentStr = new StringBuilder();
    UaFolderNode folderNode = null;
    for (final String segment : segments) {
      currentStr.append(segment);
      final NodeId folderNodeId = newNodeId(currentStr.toString());
      currentStr.append("/");

      folderNode =
          new UaFolderNode(
              getNodeContext(),
              folderNodeId,
              newQualifiedName(segment),
              LocalizedText.english(segment));
      if (!getNodeManager().containsNode(folderNode)) {
        getNodeManager().addNode(folderNode);
        folderNode.addReference(
            new Reference(
                folderNode.getNodeId(),
                Identifiers.Organizes,
                Identifiers.ObjectsFolder.expanded(),
                false));
      }
    }

    final String currentFolder = currentStr.toString();
    for (final MeasurementSchema measurementSchema : tablet.getSchemas()) {
      final String name = measurementSchema.getMeasurementId();
      final UaVariableNode node =
          new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
              .setNodeId(newNodeId(currentFolder + name))
              .setAccessLevel(AccessLevel.READ_WRITE)
              .setBrowseName(newQualifiedName(name))
              .setDisplayName(LocalizedText.english(name))
              .setDataType(Identifiers.String)
              .setTypeDefinition(Identifiers.BaseDataVariableType)
              .build();

      node.setAllowNulls(true);
      node.setValue(new DataValue(new Variant("admin was here")));

      getNodeManager().addNode(node);
      folderNode.addOrganizes(node);
    }
  }

  @Override
  public void onDataItemsCreated(List<DataItem> list) {}

  @Override
  public void onDataItemsModified(List<DataItem> list) {}

  @Override
  public void onDataItemsDeleted(List<DataItem> list) {}

  @Override
  public void onMonitoringModeChanged(List<MonitoredItem> list) {}
}
