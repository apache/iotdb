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
package org.apache.iotdb.db.metadata.mnode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * Represents an MNode which has a Measurement or Sensor attached to it.
 */
public class MeasurementMNode extends MNode {

  private static final long serialVersionUID = -1199657856921206435L;

  /**
   * measurement's Schema for one timeseries represented by current leaf node
   */
  private MeasurementSchema schema;
  private String alias;
  // tag/attribute's start offset in tag file
  private long offset = -1;

  private TimeValuePair cachedLastValuePair = null;

  /**
   * @param alias alias of measurementName
   */
  public MeasurementMNode(MNode parent, String measurementName, String alias, TSDataType dataType,
      TSEncoding encoding, CompressionType type, Map<String, String> props) {
    super(parent, measurementName);
    this.schema = new MeasurementSchema(measurementName, dataType, encoding, type, props);
    this.alias = alias;
  }

  public MeasurementMNode(MNode parent, String measurementName, MeasurementSchema schema,
      String alias) {
    super(parent, measurementName);
    this.schema = schema;
    this.alias = alias;
  }

  public MeasurementSchema getSchema() {
    return schema;
  }

  public TimeValuePair getCachedLast() {
    return cachedLastValuePair;
  }

  public synchronized void updateCachedLast(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) {
      return;
    }

    if (cachedLastValuePair == null) {
      // If no cached last, (1) a last query (2) an unseq insertion or (3) a seq insertion will update cache.
      if (!highPriorityUpdate || latestFlushedTime <= timeValuePair.getTimestamp()) {
        cachedLastValuePair =
            new TimeValuePair(timeValuePair.getTimestamp(), timeValuePair.getValue());
      }
    } else if (timeValuePair.getTimestamp() > cachedLastValuePair.getTimestamp()
        || (timeValuePair.getTimestamp() == cachedLastValuePair.getTimestamp()
        && highPriorityUpdate)) {
      cachedLastValuePair.setTimestamp(timeValuePair.getTimestamp());
      cachedLastValuePair.setValue(timeValuePair.getValue());
    }
  }

  @Override
  public String getFullPath() {
    return concatFullPath();
  }

  public void resetCache() {
    cachedLastValuePair = null;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public void setSchema(MeasurementSchema schema) {
    this.schema = schema;
  }

  @Override
  public void serializeTo(BufferedWriter bw) throws IOException {
    serializeChildren(bw);

    StringBuilder s = new StringBuilder(String.valueOf(MetadataConstant.MEASUREMENT_MNODE_TYPE));
    s.append(",").append(name).append(",");
    if (alias != null) {
      s.append(alias);
    }
    s.append(",").append(schema.getType().ordinal()).append(",");
    s.append(schema.getEncodingType().ordinal()).append(",");
    s.append(schema.getCompressor().ordinal()).append(",");
    if (schema.getProps() != null) {
      for (Map.Entry<String, String> entry : schema.getProps().entrySet()) {
        s.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
      }
    }
    s.append(",").append(offset).append(",");
    s.append(children == null ? "0" : children.size());
    bw.write(s.toString());
    bw.newLine();
  }

  /**
   * deserialize MeasuremetMNode from string array
   *
   * @param nodeInfo node information array. For example: "2,s0,speed,2,2,1,year:2020;month:jan;,-1,0"
   *                 representing: [0] nodeType [1] name [2] alias [3] TSDataType.ordinal() [4]
   *                 TSEncoding.ordinal() [5] CompressionType.ordinal() [6] props [7] offset [8]
   *                 children size
   */
  public static MeasurementMNode deserializeFrom(String[] nodeInfo) {
    String name = nodeInfo[1];
    String alias = nodeInfo[2].equals("") ? null : nodeInfo[2];
    Map<String, String> props = new HashMap<>();
    if (!nodeInfo[6].equals("")) {
      for (String propInfo : nodeInfo[6].split(";")) {
        props.put(propInfo.split(":")[0], propInfo.split(":")[1]);
      }
    }
    MeasurementSchema schema = new MeasurementSchema(name, Byte.parseByte(nodeInfo[3]),
        Byte.parseByte(nodeInfo[4]), Byte.parseByte(nodeInfo[5]), props);
    MeasurementMNode node = new MeasurementMNode(null, name, schema, alias);
    node.setOffset(Long.parseLong(nodeInfo[7]));

    return node;
  }
}