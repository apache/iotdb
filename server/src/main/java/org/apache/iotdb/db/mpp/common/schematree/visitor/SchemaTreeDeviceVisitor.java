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

package org.apache.iotdb.db.mpp.common.schematree.visitor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.MeasurementSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SchemaTreeDeviceVisitor extends SchemaTreeVisitor<DeviceSchemaInfo> {

  public SchemaTreeDeviceVisitor(SchemaNode root, PartialPath pathPattern, boolean isPrefixMatch) {
    super(root, pathPattern, isPrefixMatch);
  }

  @Override
  protected boolean acceptInternalMatchedNode(SchemaNode node) {
    return false;
  }

  @Override
  protected boolean acceptFullMatchedNode(SchemaNode node) {
    return node.isEntity();
  }

  @Override
  protected DeviceSchemaInfo generateResult(SchemaNode nextMatchedNode) {
    PartialPath path = getPartialPathFromRootToNode(nextMatchedNode);
    List<MeasurementSchemaInfo> measurementSchemaInfoList = new ArrayList<>();
    Iterator<SchemaNode> iterator = getChildrenIterator(nextMatchedNode);
    SchemaNode node;
    SchemaMeasurementNode measurementNode;
    while (iterator.hasNext()) {
      node = iterator.next();
      if (node.isMeasurement()) {
        measurementNode = node.getAsMeasurementNode();
        measurementSchemaInfoList.add(
            new MeasurementSchemaInfo(
                measurementNode.getName(),
                measurementNode.getSchema(),
                measurementNode.getAlias()));
      }
    }

    return new DeviceSchemaInfo(
        path, nextMatchedNode.getAsEntityNode().isAligned(), measurementSchemaInfoList);
  }
}
