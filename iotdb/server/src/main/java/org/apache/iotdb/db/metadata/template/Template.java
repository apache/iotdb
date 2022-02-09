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
package org.apache.iotdb.db.metadata.template;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Template {
  private String name;
  private Map<String, IMNode> directNodes;
  private boolean isDirectAligned;
  private int measurementsCount;
  private Map<String, IMeasurementSchema> schemaMap;

  public Template() {}

  /**
   * build a template from a createTemplatePlan
   *
   * @param plan createTemplatePlan
   */
  public Template(CreateTemplatePlan plan) throws IllegalPathException {
    boolean isAlign;
    schemaMap = new HashMap<>();
    name = plan.getName();
    isDirectAligned = false;
    directNodes = new HashMap<>();

    for (int i = 0; i < plan.getMeasurements().size(); i++) {
      IMeasurementSchema curSchema;
      int size = plan.getMeasurements().get(i).size();
      if (size > 1) {
        isAlign = true;
      } else {
        // If sublist of measurements has only one item,
        // but it share prefix with other aligned sublist, it will be aligned too
        String[] thisMeasurement =
            MetaUtils.splitPathToDetachedPath(plan.getMeasurements().get(i).get(0));
        String thisPrefix =
            joinBySeparator(Arrays.copyOf(thisMeasurement, thisMeasurement.length - 1));
        isAlign =
            plan.getAlignedDeviceId() != null && plan.getAlignedDeviceId().contains(thisPrefix);
      }

      // vector, aligned measurements
      if (isAlign) {
        IMeasurementSchema[] curSchemas;
        String[] measurementsArray = new String[size];
        TSDataType[] typeArray = new TSDataType[size];
        TSEncoding[] encodingArray = new TSEncoding[size];
        CompressionType[] compressorArray = new CompressionType[size];

        for (int j = 0; j < size; j++) {
          measurementsArray[j] = plan.getMeasurements().get(i).get(j);
          typeArray[j] = plan.getDataTypes().get(i).get(j);
          encodingArray[j] = plan.getEncodings().get(i).get(j);
          compressorArray[j] = plan.getCompressors().get(i).get(j);
        }

        curSchemas = constructSchemas(measurementsArray, typeArray, encodingArray, compressorArray);
        constructTemplateTree(measurementsArray, curSchemas);

      }
      // normal measurement
      else {
        curSchema =
            new UnaryMeasurementSchema(
                plan.getMeasurements().get(i).get(0),
                plan.getDataTypes().get(i).get(0),
                plan.getEncodings().get(i).get(0),
                plan.getCompressors().get(i).get(0));
        constructTemplateTree(plan.getMeasurements().get(i).get(0), curSchema);
      }
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, IMeasurementSchema> getSchemaMap() {
    return schemaMap;
  }

  public boolean hasSchema(String suffixPath) {
    return schemaMap.containsKey(suffixPath);
  }

  public IMeasurementSchema getSchema(String measurementId) {
    return schemaMap.get(measurementId);
  }

  public boolean isDirectAligned() {
    return isDirectAligned;
  }

  // region construct template tree
  /** Construct aligned measurements, checks prefix equality, path duplication and conflict */
  private void constructTemplateTree(String[] alignedPaths, IMeasurementSchema[] schemas)
      throws IllegalPathException {
    // Only for aligned Paths, with common direct prefix
    String[] pathNodes;
    IMNode commonPar;
    String prefix = null;
    List<String> measurementNames = new ArrayList<>();
    IMeasurementMNode leafNode;

    // deduplicate
    Set<String> pathSet = new HashSet<>(Arrays.asList(alignedPaths));
    if (pathSet.size() != alignedPaths.length) {
      throw new IllegalPathException("Duplication in paths.");
    }

    Set<String> checkSet = new HashSet<>();
    for (String path : alignedPaths) {
      // check aligned whether legal, and records measurements name
      if (getPathNodeInTemplate(path) != null) {
        throw new IllegalPathException("Path duplicated: " + prefix);
      }
      pathNodes = MetaUtils.splitPathToDetachedPath(path);

      if (pathNodes.length == 1) {
        prefix = "";
      } else {
        prefix = joinBySeparator(Arrays.copyOf(pathNodes, pathNodes.length - 1));
      }

      if (checkSet.size() == 0) {
        checkSet.add(prefix);
      }
      if (!checkSet.contains(prefix)) {
        throw new IllegalPathException(
            "Aligned measurements get different paths, " + alignedPaths[0]);
      }

      measurementNames.add(pathNodes[pathNodes.length - 1]);
    }

    synchronized (this) {
      if (prefix.equals("")) {
        isDirectAligned = true;
      }
      for (int i = 0; i <= measurementNames.size() - 1; i++) {
        // find the parent and add nodes to template
        if ("".equals(prefix)) {
          leafNode =
              MeasurementMNode.getMeasurementMNode(null, measurementNames.get(i), schemas[i], null);
          directNodes.put(leafNode.getName(), leafNode);
        } else {
          commonPar = constructEntityPath(alignedPaths[0]);
          commonPar.getAsEntityMNode().setAligned(true);
          leafNode =
              MeasurementMNode.getMeasurementMNode(
                  commonPar.getAsEntityMNode(), measurementNames.get(i), schemas[i], null);
          commonPar.addChild(leafNode);
        }
        schemaMap.put(getFullPathWithoutTemplateName(leafNode), schemas[i]);
        measurementsCount++;
      }
    }
  }

  /** Construct single measurement, only check path conflict and duplication */
  private IMeasurementMNode constructTemplateTree(String path, IMeasurementSchema schema)
      throws IllegalPathException {
    if (getPathNodeInTemplate(path) != null) {
      throw new IllegalPathException("Path duplicated: " + path);
    }
    String[] pathNode = MetaUtils.splitPathToDetachedPath(path);
    IMNode cur = constructEntityPath(path);

    synchronized (this) {
      IMeasurementMNode leafNode =
          MeasurementMNode.getMeasurementMNode(
              (IEntityMNode) cur, pathNode[pathNode.length - 1], schema, null);
      if (cur == null) {
        directNodes.put(leafNode.getName(), leafNode);
      } else {
        cur.addChild(leafNode);
      }
      schemaMap.put(getFullPathWithoutTemplateName(leafNode), schema);
      measurementsCount++;
      return leafNode;
    }
  }

  private IMeasurementSchema constructSchema(
      String nodeName, TSDataType dataType, TSEncoding encoding, CompressionType compressor) {
    return new UnaryMeasurementSchema(nodeName, dataType, encoding, compressor);
  }

  private IMeasurementSchema[] constructSchemas(
      String[] nodeNames,
      TSDataType[] dataTypes,
      TSEncoding[] encodings,
      CompressionType[] compressors) {
    UnaryMeasurementSchema[] schemas = new UnaryMeasurementSchema[nodeNames.length];
    for (int i = 0; i < nodeNames.length; i++) {
      schemas[i] =
          new UnaryMeasurementSchema(nodeNames[i], dataTypes[i], encodings[i], compressors[i]);
    }
    return schemas;
  }
  // endregion

  // region query of template

  public List<String> getAllAlignedPrefix() {
    List<String> alignedPrefix = new ArrayList<>();
    if (isDirectAligned) {
      alignedPrefix.add("");
    }

    Deque<IMNode> traverseChildren = new ArrayDeque<>();
    directNodes.values().forEach(traverseChildren::push);
    while (traverseChildren.size() != 0) {
      IMNode cur = traverseChildren.pop();
      if (cur.getChildren().size() != 0) {
        cur.getChildren().values().forEach(traverseChildren::push);
      }
      if (cur.isEntity() && cur.getAsEntityMNode().isAligned()) {
        alignedPrefix.add(cur.getFullPath());
      }
    }
    return alignedPrefix;
  }

  public List<String> getAlignedMeasurements(String prefix) throws IllegalPathException {
    IMNode prefixNode = getPathNodeInTemplate(prefix);
    if (prefixNode == null) {
      throw new IllegalPathException(prefix, "there is no IMNode for given prefix.");
    }
    if (prefixNode.isMeasurement()) {
      throw new IllegalPathException(prefix, "path is a measurement.");
    }
    if (!prefixNode.isEntity() || !prefixNode.getAsEntityMNode().isAligned()) {
      throw new IllegalPathException(prefix, "path has no child as aligned measurement.");
    }

    List<String> subMeasurements = new ArrayList<>();
    for (IMNode child : prefixNode.getChildren().values()) {
      if (child.isMeasurement()) {
        subMeasurements.add(child.getName());
      }
    }
    return subMeasurements;
  }

  public List<String> getAllMeasurementsPaths() {
    return new ArrayList<>(schemaMap.keySet());
  }

  public List<String> getMeasurementsUnderPath(String path) throws MetadataException {
    if ("".equals(path)) {
      return getAllMeasurementsPaths();
    }
    List<String> res = new ArrayList<>();
    IMNode cur = getPathNodeInTemplate(path);
    if (cur == null) {
      throw new PathNotExistException(path);
    }
    if (cur.isMeasurement()) {
      return Collections.singletonList(getFullPathWithoutTemplateName(cur));
    }
    Deque<IMNode> stack = new ArrayDeque<>();
    stack.push(cur);
    while (stack.size() != 0) {
      cur = stack.pop();
      if (cur.isMeasurement()) {
        res.add(getFullPathWithoutTemplateName(cur));
      } else {
        for (IMNode child : cur.getChildren().values()) {
          stack.push(child);
        }
      }
    }
    return res;
  }

  public int getMeasurementsCount() {
    return measurementsCount;
  }

  public IMNode getPathNodeInTemplate(String path) throws IllegalPathException {
    String[] pathNodes = MetaUtils.splitPathToDetachedPath(path);
    if (pathNodes.length == 0) {
      return null;
    }
    IMNode cur = directNodes.getOrDefault(pathNodes[0], null);
    if (cur == null || cur.isMeasurement()) {
      return cur;
    }
    for (int i = 1; i < pathNodes.length; i++) {
      if (cur.hasChild(pathNodes[i])) {
        cur = cur.getChild(pathNodes[i]);
      } else {
        return null;
      }
    }
    return cur;
  }

  public boolean isPathExistInTemplate(String path) throws IllegalPathException {
    String[] pathNodes = MetaUtils.splitPathToDetachedPath(path);
    if (!directNodes.containsKey(pathNodes[0])) {
      return false;
    }
    IMNode cur = directNodes.get(pathNodes[0]);
    for (int i = 1; i < pathNodes.length; i++) {
      if (cur.hasChild(pathNodes[i])) {
        cur = cur.getChild(pathNodes[i]);
      } else {
        return false;
      }
    }
    return true;
  }

  public boolean isDirectNodeInTemplate(String nodeName) {
    return directNodes.containsKey(nodeName);
  }

  public boolean isPathMeasurement(String path) throws MetadataException {
    String[] pathNodes = MetaUtils.splitPathToDetachedPath(path);
    if (!directNodes.containsKey(pathNodes[0])) {
      throw new PathNotExistException(path);
    }
    IMNode cur = directNodes.get(pathNodes[0]);
    for (int i = 1; i < pathNodes.length; i++) {
      if (cur.hasChild(pathNodes[i])) {
        cur = cur.getChild(pathNodes[i]);
      } else {
        throw new PathNotExistException(path);
      }
    }
    return cur.isMeasurement();
  }

  public IMNode getDirectNode(String nodeName) {
    return directNodes.getOrDefault(nodeName, null);
  }

  public Collection<IMNode> getDirectNodes() {
    return directNodes.values();
  }

  // endregion

  // region inner utils

  private String getFullPathWithoutTemplateName(IMNode node) {
    if (node == null) {
      return "";
    }
    StringBuilder builder = new StringBuilder(node.getName());
    IMNode cur = node.getParent();
    while (cur != null) {
      builder.insert(0, cur.getName() + TsFileConstant.PATH_SEPARATOR);
      cur = cur.getParent();
    }
    return builder.toString();
  }

  /**
   * @param path complete path to measurement.
   * @return null if need to add direct node, will never return a measurement.
   */
  private IMNode constructEntityPath(String path) throws IllegalPathException {
    String[] pathNodes = MetaUtils.splitPathToDetachedPath(path);
    if (pathNodes.length == 1) {
      return null;
    }

    IMNode cur = directNodes.get(pathNodes[0]);
    if (cur == null) {
      cur = new EntityMNode(null, pathNodes[0]);
      directNodes.put(pathNodes[0], cur);
    }

    if (cur.isMeasurement()) {
      throw new IllegalPathException(path, "there is measurement in path.");
    }

    for (int i = 1; i <= pathNodes.length - 2; i++) {
      if (!cur.hasChild(pathNodes[i])) {
        cur.addChild(pathNodes[i], new EntityMNode(cur, pathNodes[i]));
      }
      cur = cur.getChild(pathNodes[i]);

      if (cur.isMeasurement()) {
        throw new IllegalPathException(path, "there is measurement in path.");
      }
    }
    return cur;
  }

  private static String joinBySeparator(String[] pathNodes) {
    if ((pathNodes == null) || (pathNodes.length == 0)) {
      return "";
    }
    StringBuilder builder = new StringBuilder(pathNodes[0]);
    for (int i = 1; i <= pathNodes.length - 1; i++) {
      builder.append(TsFileConstant.PATH_SEPARATOR);
      builder.append(pathNodes[i]);
    }
    return builder.toString();
  }
  // endregion

  // region append of template

  public void addAlignedMeasurements(
      String[] measurements,
      TSDataType[] dataTypes,
      TSEncoding[] encodings,
      CompressionType[] compressors)
      throws IllegalPathException {
    IMeasurementSchema[] schema;
    String prefix;
    String[] pathNode;
    String[] leafNodes = new String[measurements.length];

    // If prefix exists and not aligned, it will throw exception
    // Prefix equality will be checked in constructTemplateTree
    pathNode = MetaUtils.splitPathToDetachedPath(measurements[0]);
    prefix = joinBySeparator(Arrays.copyOf(pathNode, pathNode.length - 1));
    IMNode targetNode = getPathNodeInTemplate(prefix);
    if (targetNode != null && !targetNode.getAsEntityMNode().isAligned()) {
      throw new IllegalPathException(prefix, "path already exists but not aligned");
    }

    for (int i = 0; i <= measurements.length - 1; i++) {
      pathNode = MetaUtils.splitPathToDetachedPath(measurements[i]);
      leafNodes[i] = pathNode[pathNode.length - 1];
    }
    schema = constructSchemas(leafNodes, dataTypes, encodings, compressors);
    constructTemplateTree(measurements, schema);
  }

  public void addUnalignedMeasurements(
      String[] measurements,
      TSDataType[] dataTypes,
      TSEncoding[] encodings,
      CompressionType[] compressors)
      throws IllegalPathException {
    String prefix;
    String[] pathNode;

    // deduplicate
    Set<String> pathSet = new HashSet<>(Arrays.asList(measurements));
    if (pathSet.size() != measurements.length) {
      throw new IllegalPathException("Duplication in paths.");
    }

    for (int i = 0; i <= measurements.length - 1; i++) {
      pathNode = MetaUtils.splitPathToDetachedPath(measurements[i]);

      // If prefix exists and aligned, it will throw exception
      prefix = joinBySeparator(Arrays.copyOf(pathNode, pathNode.length - 1));
      IMNode parNode = getPathNodeInTemplate(prefix);
      if (parNode != null && parNode.getAsEntityMNode().isAligned()) {
        throw new IllegalPathException(prefix, "path already exists and aligned");
      }

      IMeasurementSchema schema =
          constructSchema(
              pathNode[pathNode.length - 1], dataTypes[i], encodings[i], compressors[i]);
      constructTemplateTree(measurements[i], schema);
    }
  }

  // endregion

  // region deduction of template

  public void deleteMeasurements(String path) throws MetadataException {
    IMNode cur = getPathNodeInTemplate(path);
    if (cur == null) {
      throw new PathNotExistException(path);
    }
    if (!cur.isMeasurement()) {
      throw new IllegalPathException(path, "Path is not pointed to a measurement node.");
    }

    IMNode par = cur.getParent();
    if (par == null) {
      directNodes.remove(cur.getName());
    } else {
      par.deleteChild(cur.getName());
    }
    schemaMap.remove(getFullPathWithoutTemplateName(cur));
    measurementsCount--;
  }

  public void deleteSeriesCascade(String path) throws MetadataException {
    IMNode cur = getPathNodeInTemplate(path);
    IMNode par;

    if (cur == null) {
      throw new PathNotExistException(path);
    }
    par = cur.getParent();
    if (par == null) {
      directNodes.remove(cur.getName());
    } else {
      par.deleteChild(cur.getName());
    }

    // Remove all aligned prefix below the series path
    Deque<IMNode> astack = new ArrayDeque<>();
    astack.push(cur);
    while (astack.size() != 0) {
      IMNode top = astack.pop();
      if (!top.isMeasurement()) {
        String thisPrefix = getFullPathWithoutTemplateName(top);

        if (thisPrefix.equals("")) {
          isDirectAligned = false;
        }

        for (IMNode child : top.getChildren().values()) {
          astack.push(child);
        }
      } else {
        schemaMap.remove(getFullPathWithoutTemplateName(top));
        measurementsCount--;
      }
    }
  }

  public void deleteAlignedPrefix(String path) throws IllegalPathException {
    if (path.equals("")) {
      isDirectAligned = false;
    }
    IMNode targetNode = getPathNodeInTemplate(path);
    if (targetNode.isEntity()) {
      targetNode.getAsEntityMNode().setAligned(false);
    }
  }
  // endregion

  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    SerializeUtils.serialize(name, dataOutputStream);
    try {
      dataOutputStream.writeInt(schemaMap.size());
      for (Map.Entry<String, IMeasurementSchema> entry : schemaMap.entrySet()) {
        SerializeUtils.serialize(entry.getKey(), dataOutputStream);
        entry.getValue().partialSerializeTo(dataOutputStream);
      }
    } catch (IOException e) {
      // unreachable
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public void deserialize(ByteBuffer buffer) {
    name = SerializeUtils.deserializeString(buffer);
    int schemaSize = buffer.getInt();
    schemaMap = new HashMap<>(schemaSize);
    for (int i = 0; i < schemaSize; i++) {
      String schemaName = SerializeUtils.deserializeString(buffer);
      byte flag = ReadWriteIOUtils.readByte(buffer);
      IMeasurementSchema measurementSchema = null;
      if (flag == (byte) 0) {
        measurementSchema = UnaryMeasurementSchema.partialDeserializeFrom(buffer);
      } else if (flag == (byte) 1) {
        measurementSchema = VectorMeasurementSchema.partialDeserializeFrom(buffer);
      }
      schemaMap.put(schemaName, measurementSchema);
    }
  }

  @Override
  public boolean equals(Object t) {
    if (this == t) {
      return true;
    }
    if (t == null || getClass() != t.getClass()) {
      return false;
    }
    Template that = (Template) t;
    return this.name.equals(that.name) && this.schemaMap.equals(that.schemaMap);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(name).append(schemaMap).toHashCode();
  }
}
