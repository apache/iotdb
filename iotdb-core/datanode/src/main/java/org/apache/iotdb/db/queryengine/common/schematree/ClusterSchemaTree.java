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

package org.apache.iotdb.db.queryengine.common.schematree;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaEntityNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaInternalNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.queryengine.common.schematree.visitor.SchemaTreeDeviceUsingTemplateVisitor;
import org.apache.iotdb.db.queryengine.common.schematree.visitor.SchemaTreeDeviceVisitor;
import org.apache.iotdb.db.queryengine.common.schematree.visitor.SchemaTreeVisitorFactory;
import org.apache.iotdb.db.queryengine.common.schematree.visitor.SchemaTreeVisitorWithLimitOffsetWrapper;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaComputation;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_PATTERN;
import static org.apache.iotdb.commons.schema.SchemaConstant.NON_TEMPLATE;
import static org.apache.iotdb.db.queryengine.common.schematree.node.SchemaNode.SCHEMA_ENTITY_NODE;
import static org.apache.iotdb.db.queryengine.common.schematree.node.SchemaNode.SCHEMA_MEASUREMENT_NODE;

public class ClusterSchemaTree implements ISchemaTree {
  private static final ClusterTemplateManager templateManager =
      ClusterTemplateManager.getInstance();

  private Set<String> databases;

  private final SchemaNode root;

  /** a flag recording whether there is logical view in this schema tree. */
  private boolean hasLogicalMeasurementPath = false;

  /** used to judge schema tree type */
  private boolean hasNormalTimeSeries = false;

  private Map<Integer, Template> templateMap = new HashMap<>();

  public ClusterSchemaTree() {
    root = new SchemaInternalNode(PATH_ROOT);
  }

  public ClusterSchemaTree(SchemaNode root) {
    this.root = root;
  }

  public void setTemplateMap(Map<Integer, Template> templateMap) {
    this.templateMap = templateMap;
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
      visitor.setTemplateMap(templateMap);
      return new Pair<>(visitor.getAllResult(), visitor.getNextOffset());
    }
  }

  @Override
  public Pair<List<MeasurementPath>, Integer> searchMeasurementPaths(PartialPath pathPattern) {
    return searchMeasurementPaths(pathPattern, 0, 0, false);
  }

  public List<DeviceSchemaInfo> getAllDevices() {
    return getMatchedDevices(ALL_MATCH_PATTERN);
  }

  /**
   * Get all device matching the path pattern.
   *
   * @param pathPattern the pattern of the target devices.
   * @return A HashSet instance which stores info of the devices matching the given path pattern.
   */
  @Override
  public List<DeviceSchemaInfo> getMatchedDevices(PartialPath pathPattern) {
    return getMatchedDevices(pathPattern, false);
  }

  private List<DeviceSchemaInfo> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch) {
    try (SchemaTreeDeviceVisitor visitor =
        SchemaTreeVisitorFactory.createSchemaTreeDeviceVisitor(root, pathPattern, isPrefixMatch)) {
      return visitor.getAllResult();
    }
  }

  /**
   * Get measurement schema info from entity node children or template.
   *
   * @param entityNode entity node
   * @param measurement measurement name
   * @return measurement schema info, null if not found
   */
  private IMeasurementSchemaInfo getMeasurementSchemaInfo(
      SchemaEntityNode entityNode, String measurement) {
    SchemaNode node = entityNode.getChild(measurement);
    if (node != null && node.isMeasurement()) {
      return node.getAsMeasurementNode();
    } else {
      Template template = templateMap.get(entityNode.getTemplateId());
      if (template != null && template.getSchemaMap().containsKey(measurement)) {
        return new MeasurementSchemaInfo(
            measurement, template.getSchema(measurement), null, null, null);
      }
    }
    return null;
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

    if (cur == null || !cur.isEntity()) {
      return null;
    }
    SchemaEntityNode entityNode = cur.getAsEntityNode();
    List<IMeasurementSchemaInfo> measurementSchemaInfoList = new ArrayList<>();
    for (String measurement : measurements) {
      measurementSchemaInfoList.add(getMeasurementSchemaInfo(entityNode, measurement));
    }

    return new DeviceSchemaInfo(
        devicePath, entityNode.isAligned(), entityNode.getTemplateId(), measurementSchemaInfoList);
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
    if (cur == null || !cur.isEntity()) {
      return indexOfTargetMeasurements;
    }
    SchemaEntityNode entityNode = cur.getAsEntityNode();
    boolean firstNonViewMeasurement = true;
    List<Integer> indexOfMissingMeasurements = new ArrayList<>();
    for (int index : indexOfTargetMeasurements) {
      IMeasurementSchemaInfo measurementSchemaInfo =
          getMeasurementSchemaInfo(entityNode, measurements[index]);
      if (measurementSchemaInfo == null) {
        indexOfMissingMeasurements.add(index);
      } else {
        if (firstNonViewMeasurement && !measurementSchemaInfo.isLogicalView()) {
          schemaComputation.computeDevice(cur.getAsEntityNode().isAligned());
          firstNonViewMeasurement = false;
        }
        schemaComputation.computeMeasurement(index, measurementSchemaInfo);
      }
    }
    return indexOfMissingMeasurements;
  }

  /**
   * This function compute logical view and fill source of these views. It returns nothing ! If some
   * source paths are missed, throw errors.
   *
   * @param schemaComputation the statement
   * @param indexOfTargetLogicalView the index list of logicalViewSchemaList that you want to check
   * @throws SemanticException path not exist or different source path of view
   */
  public void computeSourceOfLogicalView(
      ISchemaComputation schemaComputation, List<Integer> indexOfTargetLogicalView)
      throws SemanticException {
    if (!schemaComputation.hasLogicalViewNeedProcess()) {
      return;
    }
    List<LogicalViewSchema> logicalViewSchemaList = schemaComputation.getLogicalViewSchemaList();
    for (Integer index : indexOfTargetLogicalView) {
      LogicalViewSchema logicalViewSchema = logicalViewSchemaList.get(index);
      PartialPath fullPath = logicalViewSchema.getSourcePathIfWritable();
      Pair<List<MeasurementPath>, Integer> searchResult = this.searchMeasurementPaths(fullPath);
      List<MeasurementPath> measurementPathList = searchResult.left;
      if (measurementPathList.isEmpty()) {
        throw new SemanticException(
            new PathNotExistException(
                fullPath.getFullPath(),
                schemaComputation
                    .getDevicePath()
                    .concatAsMeasurementPath(logicalViewSchema.getMeasurementId())
                    .getFullPath()));
      } else if (measurementPathList.size() > 1) {
        throw new SemanticException(
            String.format(
                "The source paths [%s] of view [%s] are multiple.",
                fullPath.getFullPath(),
                schemaComputation
                    .getDevicePath()
                    .concatAsMeasurementPath(logicalViewSchema.getMeasurementId())));
      } else {
        Integer realIndex = schemaComputation.getIndexListOfLogicalViewPaths().get(index);
        MeasurementPath measurementPath = measurementPathList.get(0);
        schemaComputation.computeMeasurementOfView(
            realIndex,
            new MeasurementSchemaInfo(
                measurementPath.getMeasurement(),
                measurementPath.getMeasurementSchema(),
                null,
                measurementPath.getTagMap(),
                null),
            measurementPath.isUnderAlignedEntity());
      }
    }
  }

  /**
   * Append a template device to the schema tree.
   *
   * @param devicePath device path
   * @param isAligned whether the device is aligned
   * @param templateId template id
   * @param template template instance, used to search measurement schema, it can be null if
   *     write-only
   */
  public void appendTemplateDevice(
      PartialPath devicePath, boolean isAligned, int templateId, Template template) {
    String[] nodes = devicePath.getNodes();
    SchemaNode cur = root;
    SchemaNode child;
    for (int i = 1; i < nodes.length - 1; i++) {
      child = cur.getChild(nodes[i]);
      if (child == null) {
        child = new SchemaInternalNode(nodes[i]);
        cur.addChild(nodes[i], child);
      }
      cur = child;
    }
    String deviceName = nodes[nodes.length - 1];
    child = cur.getChild(deviceName);
    if (child == null) {
      SchemaEntityNode entityNode = new SchemaEntityNode(deviceName);
      entityNode.setAligned(isAligned);
      entityNode.setTemplateId(templateId);
      cur.addChild(deviceName, entityNode);
    } else if (child.isEntity()) {
      child.getAsEntityNode().setTemplateId(templateId);
      child.getAsEntityNode().setAligned(isAligned);
    } else {
      SchemaEntityNode entityNode = new SchemaEntityNode(deviceName);
      entityNode.setAligned(isAligned);
      entityNode.setTemplateId(templateId);
      cur.replaceChild(deviceName, entityNode);
    }
    if (template != null) {
      templateMap.putIfAbsent(templateId, template);
    }
  }

  public void appendMeasurementPaths(List<MeasurementPath> measurementPathList) {
    for (MeasurementPath measurementPath : measurementPathList) {
      appendSingleMeasurementPath(measurementPath);
    }
  }

  public void appendSingleMeasurementPath(MeasurementPath measurementPath) {
    appendSingleMeasurement(
        measurementPath,
        measurementPath.getMeasurementSchema(),
        measurementPath.getTagMap(),
        null,
        measurementPath.isMeasurementAliasExists() ? measurementPath.getMeasurementAlias() : null,
        measurementPath.isUnderAlignedEntity());
  }

  public void appendSingleMeasurement(
      PartialPath path,
      IMeasurementSchema schema,
      Map<String, String> tagMap,
      Map<String, String> attributeMap,
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
          measurementNode.setAttributeMap(attributeMap);
          child = measurementNode;
          if (schema.isLogicalView()) {
            this.hasLogicalMeasurementPath = true;
          }
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
    hasNormalTimeSeries = true;
  }

  @Override
  public void mergeSchemaTree(ISchemaTree schemaTree) {
    if (schemaTree instanceof ClusterSchemaTree) {
      this.mergeSchemaTree((ClusterSchemaTree) schemaTree);
    }
  }

  @Override
  public boolean hasNormalTimeSeries() {
    return hasNormalTimeSeries;
  }

  @Override
  public List<Template> getUsingTemplates() {
    return new ArrayList<>(templateMap.values());
  }

  @Override
  public List<PartialPath> getDeviceUsingTemplate(int templateId) {
    try (SchemaTreeDeviceUsingTemplateVisitor visitor =
        SchemaTreeVisitorFactory.createSchemaTreeDeviceUsingTemplateVisitor(
            root, ALL_MATCH_PATTERN, templateId)) {
      return visitor.getAllResult();
    }
  }

  public void mergeSchemaTree(ClusterSchemaTree schemaTree) {
    this.hasLogicalMeasurementPath =
        this.hasLogicalMeasurementPath || schemaTree.hasLogicalViewMeasurement();
    traverseAndMerge(this.root, null, schemaTree.root);
    this.templateMap.putAll(schemaTree.templateMap);
    this.hasNormalTimeSeries |= schemaTree.hasNormalTimeSeries;
  }

  @Override
  public void removeLogicalView() {
    removeLogicViewMeasurement(root);
  }

  private void removeLogicViewMeasurement(SchemaNode parent) {
    if (parent.isMeasurement()) {
      return;
    }

    Map<String, SchemaNode> children = parent.getChildren();
    List<String> childrenToBeRemoved = new ArrayList<>();
    for (Map.Entry<String, SchemaNode> entry : children.entrySet()) {
      SchemaNode child = entry.getValue();
      if (child.isMeasurement() && child.getAsMeasurementNode().isLogicalView()) {
        childrenToBeRemoved.add(entry.getKey());
      } else {
        removeLogicViewMeasurement(child);
      }
    }

    for (String key : childrenToBeRemoved) {
      parent.removeChild(key);
    }
  }

  private void traverseAndMerge(SchemaNode thisNode, SchemaNode thisParent, SchemaNode thatNode) {
    SchemaNode thisChild;
    SchemaEntityNode entityNode;
    // merge template device
    if (thatNode.isEntity() && thatNode.getAsEntityNode().getTemplateId() != NON_TEMPLATE) {
      if (thisNode.isEntity()) {
        entityNode = thisNode.getAsEntityNode();
      } else {
        entityNode = new SchemaEntityNode(thisNode.getName());
        thisParent.replaceChild(thisNode.getName(), entityNode);
        thisNode = entityNode;
      }
      entityNode.setTemplateId(thatNode.getAsEntityNode().getTemplateId());
    }
    // merge normal device, internal node and measurement
    for (SchemaNode thatChild : thatNode.getChildren().values()) {
      thisChild = thisNode.getChild(thatChild.getName());
      if (thisChild == null) {
        thisNode.addChild(thatChild.getName(), thatChild);
        if (thatChild.isMeasurement()) {
          SchemaEntityNode thatEntity = thatNode.getAsEntityNode();
          if (thisNode.isEntity()) {
            entityNode = thisNode.getAsEntityNode();
          } else {
            entityNode = new SchemaEntityNode(thisNode.getName());
            thisParent.replaceChild(thisNode.getName(), entityNode);
            thisNode = entityNode;
          }

          if (!entityNode.isAligned()) {
            entityNode.setAligned(thatEntity.isAligned());
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

  @Override
  public boolean hasLogicalViewMeasurement() {
    return this.hasLogicalMeasurementPath;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    root.serialize(outputStream);
  }

  public static ClusterSchemaTree deserialize(InputStream inputStream) throws IOException {

    byte nodeType;
    int childNum;
    Deque<SchemaNode> stack = new ArrayDeque<>();
    SchemaNode child;
    boolean hasLogicalView = false;
    boolean hasNormalTimeSeries = false;
    Map<Integer, Template> templateMap = new HashMap<>();

    while (inputStream.available() > 0) {
      nodeType = ReadWriteIOUtils.readByte(inputStream);
      if (nodeType == SCHEMA_MEASUREMENT_NODE) {
        SchemaMeasurementNode measurementNode = SchemaMeasurementNode.deserialize(inputStream);
        stack.push(measurementNode);
        if (measurementNode.isLogicalView()) {
          hasLogicalView = true;
        }
        hasNormalTimeSeries = true;
      } else {
        SchemaInternalNode internalNode;
        if (nodeType == SCHEMA_ENTITY_NODE) {
          internalNode = SchemaEntityNode.deserialize(inputStream);
          int templateId = internalNode.getAsEntityNode().getTemplateId();
          if (templateId != NON_TEMPLATE) {
            templateMap.putIfAbsent(templateId, templateManager.getTemplate(templateId));
          }
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
    ClusterSchemaTree result = new ClusterSchemaTree(stack.poll());
    result.templateMap = templateMap;
    result.hasLogicalMeasurementPath = hasLogicalView;
    result.hasNormalTimeSeries = hasNormalTimeSeries;
    return result;
  }

  /**
   * Get database name by device path
   *
   * <p>e.g., root.sg1 is a database and device path = root.sg1.d1, return root.sg1
   *
   * @param deviceID only full device path, cannot be path pattern
   * @return database in the given path
   * @throws SemanticException no matched database
   */
  @Override
  public String getBelongedDatabase(IDeviceID deviceID) {
    for (String database : databases) {
      if (PathUtils.isStartWith(deviceID, database)) {
        return database;
      }
    }
    throw new SemanticException("No matched database. Please check the path " + deviceID);
  }

  @Override
  public String getBelongedDatabase(PartialPath devicePath) {
    return getBelongedDatabase(devicePath.getIDeviceIDAsFullDevice());
  }

  @Override
  public Set<String> getDatabases() {
    return databases;
  }

  @Override
  public void setDatabases(Set<String> databases) {
    this.databases = databases;
  }

  @TestOnly
  SchemaNode getRoot() {
    return root;
  }

  @Override
  public boolean isEmpty() {
    return root.getChildren() == null || root.getChildren().isEmpty();
  }
}
