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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * Represents an (Internal-)MNode which has a Measurement or Sensor attached to it.
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
  public void serializeTo(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(MetadataConstant.MEASUREMENT_MNODE_TYPE, outputStream);
    ReadWriteIOUtils.write(name, outputStream);

    ReadWriteIOUtils.writeIsNull(alias, outputStream);
    if (alias != null) {
      ReadWriteIOUtils.write(alias, outputStream);
    }
    schema.serializeTo(outputStream);
    ReadWriteIOUtils.write(offset, outputStream);
    serializeChildren(outputStream);
  }

  public static MeasurementMNode deserializeFrom(InputStream inputStream, MNode parent)
      throws IOException {
    String name = ReadWriteIOUtils.readString(inputStream);
    String alias = null;
    if (!ReadWriteIOUtils.readIsNull(inputStream)) {
      alias = ReadWriteIOUtils.readString(inputStream);
    }
    MeasurementMNode node = new MeasurementMNode(parent, name,
        MeasurementSchema.deserializeFrom(inputStream), alias);
    node.setOffset(ReadWriteIOUtils.readLong(inputStream));

    int childrenSize = ReadWriteIOUtils.readInt(inputStream);
    Map<String, MNode> children = new HashMap<>();
    for (int i = 0; i < childrenSize; i++) {
      children
          .put(ReadWriteIOUtils.readString(inputStream), MNode.deserializeFrom(inputStream, node));
    }
    node.setChildren(children);

    int aliasChildrenSize = ReadWriteIOUtils.readInt(inputStream);
    Map<String, MNode> aliasChildren = new HashMap<>();
    for (int i = 0; i < aliasChildrenSize; i++) {
      children
          .put(ReadWriteIOUtils.readString(inputStream), MNode.deserializeFrom(inputStream, node));
    }
    node.setAliasChildren(aliasChildren);

    return node;
  }
}