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

package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.apache.iotdb.db.mpp.common.schematree.SchemaNode.SCHEMA_ENTITY_NODE;
import static org.apache.iotdb.db.mpp.common.schematree.SchemaNode.SCHEMA_MEASUREMENT_NODE;

public class SchemaTree {

  private final SchemaNode root;

  public SchemaTree(SchemaNode root) {
    this.root = root;
  }

  /**
   * Return all measurement paths for given path pattern and filter the result by slimit and offset.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return Left: all measurement paths; Right: remaining series offset
   */
  public Pair<List<MeasurementPath>, Integer> searchMeasurementPaths(
      PartialPath pathPattern, int slimit, int soffset, boolean isPrefixMatch) {
    SchemaTreeVisitor visitor =
        new SchemaTreeVisitor(root, pathPattern, slimit, soffset, isPrefixMatch);
    return new Pair<>(visitor.getAllResult(), visitor.getNextOffset());
  }

  public DeviceSchemaInfo searchDeviceSchemaInfo(
      PartialPath devicePath, List<String> measurements) {

    String[] nodes = devicePath.getNodes();
    SchemaNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }

    List<MeasurementSchema> measurementSchemaList = new ArrayList<>();
    for (String measurement : measurements) {
      measurementSchemaList.add(cur.getChild(measurement).getAsMeasurementNode().getSchema());
    }

    return new DeviceSchemaInfo(
        devicePath, cur.getAsEntityNode().isAligned(), measurementSchemaList);
  }

  public void serialize(ByteBuffer buffer) throws IOException {
    root.serialize(buffer);
  }

  public static SchemaTree deserialize(ByteBuffer buffer) {

    byte nodeType;
    int childNum;
    Deque<SchemaNode> stack = new ArrayDeque<>();
    SchemaNode child;

    while (buffer.hasRemaining()) {
      nodeType = ReadWriteIOUtils.readByte(buffer);
      if (nodeType == SCHEMA_MEASUREMENT_NODE) {
        SchemaMeasurementNode measurementNode = SchemaMeasurementNode.deserialize(buffer);
        stack.push(measurementNode);
      } else {
        SchemaInternalNode internalNode;
        if (nodeType == SCHEMA_ENTITY_NODE) {
          internalNode = SchemaEntityNode.deserialize(buffer);
        } else {
          internalNode = SchemaInternalNode.deserialize(buffer);
        }

        childNum = ReadWriteIOUtils.readInt(buffer);
        while (childNum > 0) {
          child = stack.pop();
          internalNode.addChild(child.getName(), child);
          if (child.isMeasurement()) {
            SchemaMeasurementNode measurementNode = child.getAsMeasurementNode();
            if (measurementNode.getAlias() != null) {
              internalNode
                  .getAsEntityNode()
                  .addAliasChild(measurementNode.getAlias(), measurementNode);
            }
          }
          childNum--;
        }
        stack.push(internalNode);
      }
    }
    return new SchemaTree(stack.poll());
  }
}
