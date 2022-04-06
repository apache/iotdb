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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write;

import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class CreateTimeSeriesNode extends PlanNode {
  private PartialPath path;
  private TSDataType dataType;
  private TSEncoding encoding;
  private CompressionType compressor;
  private String alias;
  private Map<String, String> props = null;
  private Map<String, String> tags = null;
  private Map<String, String> attributes = null;
  private long tagOffset = -1;

  public CreateTimeSeriesNode(
      PlanNodeId id,
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String alias) {
    super(id);
    this.path = path;
    this.dataType = dataType;
    this.encoding = encoding;
    this.compressor = compressor;
    this.tags = tags;
    this.attributes = attributes;
    this.alias = alias;
    if (props != null) {
      this.props = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      this.props.putAll(props);
    }
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  public void setCompressor(CompressionType compressor) {
    this.compressor = compressor;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public void setEncoding(TSEncoding encoding) {
    this.encoding = encoding;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  public long getTagOffset() {
    return tagOffset;
  }

  public void setTagOffset(long tagOffset) {
    this.tagOffset = tagOffset;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("Clone of CreateTimeSeriesNode is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.CREATE_TIME_SERIES.serialize(byteBuffer);
    if (path instanceof MeasurementPath) {
      ReadWriteIOUtils.write((byte)0, byteBuffer);
    } else if (path instanceof AlignedPath) {
      ReadWriteIOUtils.write((byte)1, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte)2, byteBuffer);
    }
    path.serialize(byteBuffer);
    dataType.serializeTo(byteBuffer);
    ReadWriteIOUtils.write(encoding.serialize(), byteBuffer);
    ReadWriteIOUtils.write(compressor.serialize(), byteBuffer);
    ReadWriteIOUtils.write(alias, byteBuffer);
    ReadWriteIOUtils.write(props, byteBuffer);
    ReadWriteIOUtils.write(tags, byteBuffer);
    ReadWriteIOUtils.write(attributes, byteBuffer);
    ReadWriteIOUtils.write(tagOffset, byteBuffer);
  }

  public static CreateTimeSeriesNode deserialize(ByteBuffer byteBuffer) {
    byte pathType = ReadWriteIOUtils.readByte(byteBuffer);
    PartialPath partialPath = null;
    if (pathType == 0) {
      partialPath = MeasurementPath.deserialize(byteBuffer);
    } else if (pathType == 1) {
      partialPath = AlignedPath.deserialize(byteBuffer);
    } else {
      partialPath = PartialPath.deserialize(byteBuffer);
    }
    TSDataType dataType = TSDataType.deserializeFrom(byteBuffer);
    TSEncoding encoding = TSEncoding.deserialize(ReadWriteIOUtils.readByte(byteBuffer));
    CompressionType compressor = CompressionType.deserialize(ReadWriteIOUtils.readByte(byteBuffer));
    String alias = ReadWriteIOUtils.readString(byteBuffer);
    Map<String, String> props = ReadWriteIOUtils.readMap(byteBuffer);
    Map<String, String> tags = ReadWriteIOUtils.readMap(byteBuffer);
    Map<String, String> attributes = ReadWriteIOUtils.readMap(byteBuffer);
    long tagOffset = ReadWriteIOUtils.readLong(byteBuffer);
    CreateTimeSeriesNode createTimeSeriesNode = new CreateTimeSeriesNode(PlanNodeId.deserialize(byteBuffer),
        partialPath, dataType, encoding, compressor, props, tags, attributes, alias);
    createTimeSeriesNode.setTagOffset(tagOffset);
    return createTimeSeriesNode;
  }
}
