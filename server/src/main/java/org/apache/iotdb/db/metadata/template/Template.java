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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Template implements Serializable {

  private int id;
  private String name;
  private boolean isDirectAligned;
  private Map<String, IMeasurementSchema> schemaMap;

  private transient int rehashCode;

  public Template() {
    schemaMap = new HashMap<>();
  }

  public Template(
          String name,
          List<String> measurements,
          List<TSDataType> dataTypes,
          List<TSEncoding> encodings,
          List<CompressionType> compressors)
          throws IllegalPathException {
    this(name, measurements, dataTypes, encodings, compressors, false);
  }

  public Template(
          String name,
          List<String> measurements,
          List<TSDataType> dataTypes,
          List<TSEncoding> encodings,
          List<CompressionType> compressors,
          boolean isAligned)
          throws IllegalPathException {
    this.isDirectAligned = isAligned;
    this.schemaMap = new HashMap<>();
    this.name = name;
    for (int i = 0; i < measurements.size(); i++) {
      IMeasurementSchema schema = new MeasurementSchema(measurements.get(i),dataTypes.get(i),encodings.get(i),compressors.get(i));
      schemaMap.put(schema.getMeasurementId(), schema);
    }
  }

  public Template(
      String name,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<List<CompressionType>> compressors,
      Set<String> alignedDeviceId)
      throws IllegalPathException {
    // for compatibly
    boolean isAlign = false;
    for (int i = 0; i < measurements.size(); i++) {
      if(measurements.get(i).size()>1){
        isAlign = true;
      }
    }

  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
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
        throw new IllegalPathException("Path duplicated: " + path);
      }
      pathNodes = PathUtils.splitPathToDetachedNodes(path);

      if (pathNodes.length == 1) {
        prefix = "";
      } else {
        prefix = joinBySeparator(Arrays.copyOf(pathNodes, pathNodes.length - 1));
      }

      if (checkSet.isEmpty()) {
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
      }
    }
  }

  /** Construct single measurement, only check path conflict and duplication */
  private IMeasurementMNode constructTemplateTree(String path, IMeasurementSchema schema)
      throws IllegalPathException {
    if (getPathNodeInTemplate(path) != null) {
      throw new IllegalPathException("Path duplicated: " + path);
    }
    String[] pathNode = PathUtils.splitPathToDetachedNodes(path);
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
      return leafNode;
    }
  }

  private IMeasurementSchema constructSchema(
      String nodeName, TSDataType dataType, TSEncoding encoding, CompressionType compressor) {
    return new MeasurementSchema(nodeName, dataType, encoding, compressor);
  }

  private IMeasurementSchema[] constructSchemas(
      String[] nodeNames,
      TSDataType[] dataTypes,
      TSEncoding[] encodings,
      CompressionType[] compressors)
      throws IllegalPathException {
    MeasurementSchema[] schemas = new MeasurementSchema[nodeNames.length];
    for (int i = 0; i < nodeNames.length; i++) {
      schemas[i] =
          new MeasurementSchema(
              new PartialPath(nodeNames[i]).getMeasurement(),
              dataTypes[i],
              encodings[i],
              compressors[i]);
    }
    return schemas;
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
    pathNode = PathUtils.splitPathToDetachedNodes(measurements[0]);
    prefix = joinBySeparator(Arrays.copyOf(pathNode, pathNode.length - 1));
    IMNode targetNode = getPathNodeInTemplate(prefix);
    if ((targetNode != null && !targetNode.getAsEntityMNode().isAligned())
        || (prefix.equals("") && !this.isDirectAligned())) {
      throw new IllegalPathException(prefix, "path already exists but not aligned");
    }

    for (int i = 0; i <= measurements.length - 1; i++) {
      pathNode = PathUtils.splitPathToDetachedNodes(measurements[i]);
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
      pathNode = PathUtils.splitPathToDetachedNodes(measurements[i]);

      // If prefix exists and aligned, it will throw exception
      prefix = joinBySeparator(Arrays.copyOf(pathNode, pathNode.length - 1));
      IMNode parNode = getPathNodeInTemplate(prefix);
      if ((parNode != null && parNode.getAsEntityMNode().isAligned())
          || (prefix.equals("") && this.isDirectAligned())) {
        throw new IllegalPathException(measurements[i], "path already exists and aligned");
      }

      IMeasurementSchema schema =
          constructSchema(
              pathNode[pathNode.length - 1], dataTypes[i], encodings[i], compressors[i]);
      constructTemplateTree(measurements[i], schema);
    }
  }

  // endregion

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(id, buffer);
    ReadWriteIOUtils.write(name, buffer);
    ReadWriteIOUtils.write(isDirectAligned, buffer);
    ReadWriteIOUtils.write(schemaMap.size(), buffer);
    for (Map.Entry<String, IMeasurementSchema> entry : schemaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), buffer);
      entry.getValue().partialSerializeTo(buffer);
    }
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(id, outputStream);
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(isDirectAligned, outputStream);
    ReadWriteIOUtils.write(schemaMap.size(), outputStream);
    for (Map.Entry<String, IMeasurementSchema> entry : schemaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().partialSerializeTo(outputStream);
    }
  }

  public ByteBuffer serialize() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      serialize(outputStream);
    } catch (IOException ignored) {

    }
    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  public void deserialize(ByteBuffer buffer) {
    id = ReadWriteIOUtils.readInt(buffer);
    name = ReadWriteIOUtils.readString(buffer);
    isDirectAligned = ReadWriteIOUtils.readBool(buffer);
    int schemaSize = ReadWriteIOUtils.readInt(buffer);
    schemaMap = new HashMap<>(schemaSize);
    for (int i = 0; i < schemaSize; i++) {
      String schemaName = ReadWriteIOUtils.readString(buffer);
      byte flag = ReadWriteIOUtils.readByte(buffer);
      IMeasurementSchema measurementSchema = null;
      if (flag == (byte) 0) {
        measurementSchema = MeasurementSchema.partialDeserializeFrom(buffer);
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
    return rehashCode != 0
        ? rehashCode
        : new HashCodeBuilder(17, 37).append(name).append(schemaMap).toHashCode();
  }
}
