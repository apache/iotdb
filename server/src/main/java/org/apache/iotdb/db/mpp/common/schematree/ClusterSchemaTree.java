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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaEntityNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaInternalNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.mpp.common.schematree.visitor.SchemaTreeDeviceVisitor;
import org.apache.iotdb.db.mpp.common.schematree.visitor.SchemaTreeVisitorFactory;
import org.apache.iotdb.db.mpp.common.schematree.visitor.SchemaTreeVisitorWithLimitOffsetWrapper;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaComputation;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_MATCH_PATTERN;
import static org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode.SCHEMA_ENTITY_NODE;
import static org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode.SCHEMA_MEASUREMENT_NODE;

public class ClusterSchemaTree implements ISchemaTree {

  private Set<String> databases;

  private final SchemaNode root;

  public ClusterSchemaTree() {
    root = new SchemaInternalNode(PATH_ROOT);
  }

  public ClusterSchemaTree(SchemaNode root) {
    this.root = root;
  }

  /**
   * Return all measurement paths for given path pattern and filter the result by slimit and offset.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return Left: all measurement paths; Right: remaining series offset
   */
  @Override
  public Pair<List<MeasurementPath>, Integer> searchMeasurementPaths(
      PartialPath pathPattern, int slimit, int soffset, boolean isPrefixMatch) {
    try (SchemaTreeVisitorWithLimitOffsetWrapper<MeasurementPath> visitor =
        SchemaTreeVisitorFactory.createSchemaTreeMeasurementVisitor(
            root, pathPattern, isPrefixMatch, slimit, soffset)) {
      return new Pair<>(visitor.getAllResult(), visitor.getNextOffset());
    }
  }

  @Override
  public Pair<List<MeasurementPath>, Integer> searchMeasurementPaths(PartialPath pathPattern) {
    try (SchemaTreeVisitorWithLimitOffsetWrapper<MeasurementPath> visitor =
        SchemaTreeVisitorFactory.createSchemaTreeMeasurementVisitor(
            root, pathPattern, false, 0, 0)) {
      return new Pair<>(visitor.getAllResult(), visitor.getNextOffset());
    }
  }

  public List<MeasurementPath> getAllMeasurement() {
    return searchMeasurementPaths(ALL_MATCH_PATTERN, 0, 0, false).left;
  }

  /**
   * Get all device matching the path pattern.
   *
   * @param pathPattern the pattern of the target devices.
   * @return A HashSet instance which stores info of the devices matching the given path pattern.
   */
  @Override
  public List<DeviceSchemaInfo> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch) {
    try (SchemaTreeDeviceVisitor visitor =
        SchemaTreeVisitorFactory.createSchemaTreeDeviceVisitor(root, pathPattern, isPrefixMatch)) {
      return visitor.getAllResult();
    }
  }

  @Override
  public List<DeviceSchemaInfo> getMatchedDevices(PartialPath pathPattern) {
    try (SchemaTreeDeviceVisitor visitor =
        SchemaTreeVisitorFactory.createSchemaTreeDeviceVisitor(root, pathPattern, false)) {
      return visitor.getAllResult();
    }
  }

  @Override
  public DeviceSchemaInfo searchDeviceSchemaInfo(
      PartialPath devicePath, List<String> measurements) {

    String[] nodes = devicePath.getNodes();
    SchemaNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      if (cur == null) {
        return null;
      }
      cur = cur.getChild(nodes[i]);
    }

    if (cur == null) {
      return null;
    }

    List<MeasurementSchemaInfo> measurementSchemaInfoList = new ArrayList<>();
    SchemaNode node;
    SchemaMeasurementNode measurementNode;
    for (String measurement : measurements) {
      node = cur.getChild(measurement);
      if (node == null) {
        measurementSchemaInfoList.add(null);
      } else {
        measurementNode = node.getAsMeasurementNode();
        measurementSchemaInfoList.add(
            new MeasurementSchemaInfo(
                measurementNode.getName(),
                measurementNode.getSchema(),
                measurementNode.getAlias()));
      }
    }

    return new DeviceSchemaInfo(
        devicePath, cur.getAsEntityNode().isAligned(), measurementSchemaInfoList);
  }

  public List<Integer> compute(
      ISchemaComputation schemaComputation, List<Integer> indexOfTargetMeasurements) {
    PartialPath devicePath = schemaComputation.getDevicePath();
    String[] measurements = schemaComputation.getMeasurements();

    String[] nodes = devicePath.getNodes();
    SchemaNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      if (cur == null) {
        return indexOfTargetMeasurements;
      }
      cur = cur.getChild(nodes[i]);
    }
    if (cur == null) {
      return indexOfTargetMeasurements;
    }
    if (cur.isEntity()) {
      schemaComputation.computeDevice(cur.getAsEntityNode().isAligned());
    }
    List<Integer> indexOfMissingMeasurements = new ArrayList<>();
    SchemaNode node;
    for (int index : indexOfTargetMeasurements) {
      node = cur.getChild(measurements[index]);
      if (node == null) {
        indexOfMissingMeasurements.add(index);
      } else {
        schemaComputation.computeMeasurement(index, node.getAsMeasurementNode());
      }
    }
    return indexOfMissingMeasurements;
  }

  public void appendMeasurementPaths(List<MeasurementPath> measurementPathList) {
    for (MeasurementPath measurementPath : measurementPathList) {
      appendSingleMeasurementPath(measurementPath);
    }
  }

  private void appendSingleMeasurementPath(MeasurementPath measurementPath) {
    appendSingleMeasurement(
        measurementPath,
        (MeasurementSchema) measurementPath.getMeasurementSchema(),
        measurementPath.getTagMap(),
        measurementPath.isMeasurementAliasExists() ? measurementPath.getMeasurementAlias() : null,
        measurementPath.isUnderAlignedEntity());
  }

  public void appendSingleMeasurement(
      PartialPath path,
      MeasurementSchema schema,
      Map<String, String> tagMap,
      String alias,
      boolean isAligned) {
    String[] nodes = path.getNodes();
    SchemaNode cur = root;
    SchemaNode child;
    for (int i = 1; i < nodes.length; i++) {
      child = cur.getChild(nodes[i]);
      if (child == null) {
        if (i == nodes.length - 1) {
          SchemaMeasurementNode measurementNode = new SchemaMeasurementNode(nodes[i], schema);
          if (alias != null) {
            measurementNode.setAlias(alias);
            cur.getAsEntityNode().addAliasChild(alias, measurementNode);
          }
          measurementNode.setTagMap(tagMap);
          child = measurementNode;
        } else if (i == nodes.length - 2) {
          SchemaEntityNode entityNode = new SchemaEntityNode(nodes[i]);
          entityNode.setAligned(isAligned);
          child = entityNode;
        } else {
          child = new SchemaInternalNode(nodes[i]);
        }
        cur.addChild(nodes[i], child);
      } else if (i == nodes.length - 2 && !child.isEntity()) {
        SchemaEntityNode entityNode = new SchemaEntityNode(nodes[i]);
        cur.replaceChild(nodes[i], entityNode);
        if (!entityNode.isAligned()) {
          entityNode.setAligned(isAligned);
        }
        child = entityNode;
      }
      cur = child;
    }
  }

  public void mergeSchemaTree(ClusterSchemaTree schemaTree) {
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

  public void serialize(OutputStream outputStream) throws IOException {
    root.serialize(outputStream);
  }

  public static ClusterSchemaTree deserialize(InputStream inputStream) throws IOException {

    byte nodeType;
    int childNum;
    Deque<SchemaNode> stack = new ArrayDeque<>();
    SchemaNode child;

    while (inputStream.available() > 0) {
      nodeType = ReadWriteIOUtils.readByte(inputStream);
      if (nodeType == SCHEMA_MEASUREMENT_NODE) {
        SchemaMeasurementNode measurementNode = SchemaMeasurementNode.deserialize(inputStream);
        stack.push(measurementNode);
      } else {
        SchemaInternalNode internalNode;
        if (nodeType == SCHEMA_ENTITY_NODE) {
          internalNode = SchemaEntityNode.deserialize(inputStream);
        } else {
          internalNode = SchemaInternalNode.deserialize(inputStream);
        }

        childNum = ReadWriteIOUtils.readInt(inputStream);
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
    return new ClusterSchemaTree(stack.poll());
  }

  /**
   * Get database name by path
   *
   * <p>e.g., root.sg1 is a database and path = root.sg1.d1, return root.sg1
   *
   * @param pathName only full path, cannot be path pattern
   * @return database in the given path
   */
  @Override
  public String getBelongedDatabase(String pathName) {
    for (String database : databases) {
      if (PathUtils.isStartWith(pathName, database)) {
        return database;
      }
    }
    throw new RuntimeException("No matched database. Please check the path " + pathName);
  }

  @Override
  public String getBelongedDatabase(PartialPath path) {
    return getBelongedDatabase(path.getFullPath());
  }

  @Override
  public Set<String> getDatabases() {
    return databases;
  }

  public void setDatabases(Set<String> databases) {
    this.databases = databases;
  }

  @TestOnly
  SchemaNode getRoot() {
    return root;
  }

  @Override
  public boolean isEmpty() {
    return root.getChildren() == null || root.getChildren().size() == 0;
  }
}
