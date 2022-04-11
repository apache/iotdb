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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.Executor.NoQueryExecutor;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.SchemaRegion;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CreateTimeSeriesNode extends PlanNode implements NoQueryExecutor {
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
  public PhysicalPlan transferToPhysicalPlan() {
    return new CreateTimeSeriesPlan(
        getPath(),
        getDataType(),
        getEncoding(),
        getCompressor(),
        getProps(),
        getTags(),
        getAttributes(),
        getAlias());
  }

  public static CreateTimeSeriesNode deserialize(ByteBuffer byteBuffer)
      throws IllegalPathException {
    String id;
    PartialPath path = null;
    TSDataType dataType;
    TSEncoding encoding;
    CompressionType compressor;
    long tagOffset;
    String alias = null;
    Map<String, String> props = null;
    Map<String, String> tags = null;
    Map<String, String> attributes = null;

    id = ReadWriteIOUtils.readString(byteBuffer);
    int length = byteBuffer.getInt();
    byte[] bytes = new byte[length];
    byteBuffer.get(bytes);
    path = new PartialPath(new String(bytes));
    dataType = TSDataType.values()[byteBuffer.get()];
    encoding = TSEncoding.values()[byteBuffer.get()];
    compressor = CompressionType.values()[byteBuffer.get()];
    tagOffset = byteBuffer.getLong();

    // alias
    if (byteBuffer.get() == 1) {
      alias = ReadWriteIOUtils.readString(byteBuffer);
    }

    byte label = byteBuffer.get();
    // props
    if (label == 0) {
      props = new HashMap<>();
    } else if (label == 1) {
      props = ReadWriteIOUtils.readMap(byteBuffer);
    }

    // tags
    label = byteBuffer.get();
    if (label == 0) {
      tags = new HashMap<>();
    } else if (label == 1) {
      tags = ReadWriteIOUtils.readMap(byteBuffer);
    }

    // attributes
    label = byteBuffer.get();
    if (label == 0) {
      attributes = new HashMap<>();
    } else if (label == 1) {
      attributes = ReadWriteIOUtils.readMap(byteBuffer);
    }

    return new CreateTimeSeriesNode(
        new PlanNodeId(id), path, dataType, encoding, compressor, props, tags, attributes, alias);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    byteBuffer.putShort((short) PlanNodeType.CREATE_TIME_SERIES.ordinal());
    ReadWriteIOUtils.write(this.getPlanNodeId().getId(), byteBuffer);
    byte[] bytes = path.getFullPath().getBytes();
    byteBuffer.putInt(bytes.length);
    byteBuffer.put(bytes);
    byteBuffer.put((byte) dataType.ordinal());
    byteBuffer.put((byte) encoding.ordinal());
    byteBuffer.put((byte) compressor.ordinal());
    byteBuffer.putLong(tagOffset);

    // alias
    if (alias != null) {
      byteBuffer.put((byte) 1);
      ReadWriteIOUtils.write(alias, byteBuffer);
    } else {
      byteBuffer.put((byte) 0);
    }

    // props
    if (props == null) {
      byteBuffer.put((byte) -1);
    } else if (props.isEmpty()) {
      byteBuffer.put((byte) 0);
    } else {
      byteBuffer.put((byte) 1);
      ReadWriteIOUtils.write(props, byteBuffer);
    }

    // tags
    if (tags == null) {
      byteBuffer.put((byte) -1);
    } else if (tags.isEmpty()) {
      byteBuffer.put((byte) 0);
    } else {
      byteBuffer.put((byte) 1);
      ReadWriteIOUtils.write(tags, byteBuffer);
    }

    // attributes
    if (attributes == null) {
      byteBuffer.put((byte) -1);
    } else if (attributes.isEmpty()) {
      byteBuffer.put((byte) 0);
    } else {
      byteBuffer.put((byte) 1);
      ReadWriteIOUtils.write(attributes, byteBuffer);
    }

    // no children node, need to set 0
    byteBuffer.putInt(0);
  }

  @Override
  public void executor(SchemaRegion schemaRegion) throws MetadataException {
    schemaRegion.createTimeseries((CreateTimeSeriesPlan) transferToPhysicalPlan());
  }
}
