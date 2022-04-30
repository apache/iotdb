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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaEntityNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaInternalNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.mpp.common.schematree.visitor.SchemaTreeDeviceVisitor;
import org.apache.iotdb.db.mpp.common.schematree.visitor.SchemaTreeMeasurementVisitor;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode.SCHEMA_ENTITY_NODE;
import static org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode.SCHEMA_MEASUREMENT_NODE;

public class SchemaTree {

  private List<String> storageGroups;

  private final SchemaNode root;

  public SchemaTree() {
    root = new SchemaInternalNode(PATH_ROOT);
  }

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
    SchemaTreeMeasurementVisitor visitor =
        new SchemaTreeMeasurementVisitor(root, pathPattern, slimit, soffset, isPrefixMatch);
    return new Pair<>(visitor.getAllResult(), visitor.getNextOffset());
  }

  /**
   * Get all device matching the path pattern.
   *
   * @param pathPattern the pattern of the target devices.
   * @return A HashSet instance which stores info of the devices matching the given path pattern.
   */
  public List<DeviceSchemaInfo> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    SchemaTreeDeviceVisitor visitor = new SchemaTreeDeviceVisitor(root, pathPattern, isPrefixMatch);
    return visitor.getAllResult();
  }

  public DeviceSchemaInfo searchDeviceSchemaInfo(
      PartialPath devicePath, List<String> measurements) {

    String[] nodes = devicePath.getNodes();
    SchemaNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }

    List<SchemaMeasurementNode> measurementNodeList = new ArrayList<>();
    for (String measurement : measurements) {
      measurementNodeList.add(cur.getChild(measurement).getAsMeasurementNode());
    }

    return new DeviceSchemaInfo(devicePath, cur.getAsEntityNode().isAligned(), measurementNodeList);
  }

  public void appendMeasurementPaths(List<MeasurementPath> measurementPathList) {
    for (MeasurementPath measurementPath : measurementPathList) {
      appendSingleMeasurementPath(measurementPath);
    }
  }

  private void appendSingleMeasurementPath(MeasurementPath measurementPath) {
    String[] nodes = measurementPath.getNodes();
    SchemaNode cur = root;
    SchemaNode child;
    for (int i = 1; i < nodes.length; i++) {
      child = cur.getChild(nodes[i]);
      if (child == null) {
        if (i == nodes.length - 1) {
          SchemaMeasurementNode measurementNode =
              new SchemaMeasurementNode(
                  nodes[i], (MeasurementSchema) measurementPath.getMeasurementSchema());
          if (measurementPath.isMeasurementAliasExists()) {
            measurementNode.setAlias(measurementPath.getMeasurementAlias());
            cur.getAsEntityNode()
                .addAliasChild(measurementPath.getMeasurementAlias(), measurementNode);
          }
          child = measurementNode;
        } else if (i == nodes.length - 2) {
          SchemaEntityNode entityNode = new SchemaEntityNode(nodes[i]);
          entityNode.setAligned(measurementPath.isUnderAlignedEntity());
          child = entityNode;
        } else {
          child = new SchemaInternalNode(nodes[i]);
        }
        cur.addChild(nodes[i], child);
      } else if (i == nodes.length - 2 && !child.isEntity()) {
        SchemaEntityNode entityNode = new SchemaEntityNode(nodes[i]);
        cur.replaceChild(nodes[i], entityNode);
        child = entityNode;
      }
      cur = child;
    }
  }

  public void mergeSchemaTree(SchemaTree schemaTree) {
    traverseAndMerge(this.root, null, schemaTree.root);
  }

  private void traverseAndMerge(SchemaNode thisNode, SchemaNode thisParent, SchemaNode thatNode) {
    SchemaNode thisChild;
    for (SchemaNode thatChild : thatNode.getChildren().values()) {
      thisChild = thisNode.getChild(thatChild.getName());
      if (thisChild == null) {
        thisNode.addChild(thatChild.getName(), thatChild);
        if (thatChild.isMeasurement()) {
          SchemaEntityNode entityNode;
          if (thisNode.isEntity()) {
            entityNode = thisNode.getAsEntityNode();
          } else {
            entityNode = new SchemaEntityNode(thisNode.getName());
            thisParent.replaceChild(thisNode.getName(), entityNode);
            thisNode = entityNode;
          }

          if (!entityNode.isAligned()) {
            entityNode.setAligned(thatNode.getAsEntityNode().isAligned());
          }
          SchemaMeasurementNode measurementNode = thatChild.getAsMeasurementNode();
          if (measurementNode.getAlias() != null) {
            entityNode.addAliasChild(measurementNode.getAlias(), measurementNode);
          }
        }
      } else {
        traverseAndMerge(thisChild, thisNode, thatChild);
      }
    }
  }

  public void serialize(ByteBuffer buffer) {
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

  /**
   * Get storage group name by path
   *
   * <p>e.g., root.sg1 is a storage group and path = root.sg1.d1, return root.sg1
   *
   * @param path only full path, cannot be path pattern
   * @return storage group in the given path
   */
  public String getBelongedStorageGroup(PartialPath path) {
    for (String storageGroup : storageGroups) {
      if (path.getFullPath().startsWith(storageGroup + ".")) {
        return storageGroup;
      }
    }
    throw new RuntimeException(
        "No matched storage group. Please check the path " + path.getFullPath());
  }

  public List<String> getStorageGroups() {
    return storageGroups;
  }

  public void setStorageGroups(List<String> storageGroups) {
    this.storageGroups = storageGroups;
  }

  @TestOnly
  SchemaNode getRoot() {
    return root;
  }
}
