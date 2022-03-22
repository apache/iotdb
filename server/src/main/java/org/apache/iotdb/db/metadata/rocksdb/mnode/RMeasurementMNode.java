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
package org.apache.iotdb.db.metadata.rocksdb.mnode;

import org.apache.iotdb.db.engine.trigger.executor.TriggerExecutor;
import org.apache.iotdb.db.metadata.lastCache.container.ILastCacheContainer;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.rocksdb.RSchemaConstants;
import org.apache.iotdb.db.metadata.rocksdb.RSchemaUtils;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class RMeasurementMNode extends RMNode implements IMeasurementMNode {

  protected String alias;

  private IMeasurementSchema schema;

  private Map<String, String> tags;

  private Map<String, String> attributes;

  /**
   * Constructor of MNode.
   *
   * @param fullPath
   */
  public RMeasurementMNode(String fullPath) {
    super(fullPath);
  }

  public RMeasurementMNode(String fullPath, byte[] value) {
    super(fullPath);
    deserialize(value);
  }

  @Override
  public IEntityMNode getParent() {
    if (super.getParent() == null) {
      return null;
    }
    return parent.getAsEntityMNode();
  }

  @Override
  public MeasurementPath getMeasurementPath() {
    MeasurementPath result = new MeasurementPath(super.getPartialPath(), schema);
    result.setUnderAlignedEntity(getParent().isAligned());
    if (alias != null && !alias.isEmpty()) {
      result.setMeasurementAlias(alias);
    }
    return result;
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public TSDataType getDataType(String measurementId) {
    return schema.getType();
  }

  // unsupported exceptions
  @Override
  public long getOffset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setOffset(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public TriggerExecutor getTriggerExecutor() {
    return null;
  }

  @Override
  public void setTriggerExecutor(TriggerExecutor triggerExecutor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ILastCacheContainer getLastCacheContainer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLastCacheContainer(ILastCacheContainer lastCacheContainer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {
    throw new UnsupportedOperationException();
  }

  private void deserialize(byte[] value) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(value);
    // skip the version flag and node type flag
    ReadWriteIOUtils.readBytes(byteBuffer, 2);

    while (byteBuffer.hasRemaining()) {
      byte blockType = ReadWriteIOUtils.readByte(byteBuffer);
      switch (blockType) {
        case RSchemaConstants.DATA_BLOCK_TYPE_ALIAS:
          alias = ReadWriteIOUtils.readString(byteBuffer);
          break;
        case RSchemaConstants.DATA_BLOCK_TYPE_SCHEMA:
          schema = MeasurementSchema.deserializeFrom(byteBuffer);
          break;
        case RSchemaConstants.DATA_BLOCK_TYPE_TAGS:
          tags = ReadWriteIOUtils.readMap(byteBuffer);
          break;
        case RSchemaConstants.DATA_BLOCK_TYPE_ATTRIBUTES:
          attributes = ReadWriteIOUtils.readMap(byteBuffer);
          break;
        default:
          break;
      }
    }
  }

  @Override
  public boolean hasChild(String name) {
    return false;
  }

  @Override
  public IMNode getChild(String name) {
    throw new RuntimeException(
        String.format(
            "current node %s is a MeasurementMNode, can not get child %s", super.name, name));
  }

  @Override
  public IMNode addChild(String name, IMNode child) {
    // Do nothing
    return child;
  }

  @Override
  public IMNode addChild(IMNode child) {
    return null;
  }

  @Override
  public void deleteChild(String name) {
    // Do nothing
  }

  @Override
  public void replaceChild(String oldChildName, IMNode newChildNode) {}

  @Override
  public Map<String, IMNode> getChildren() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setChildren(Map<String, IMNode> children) {
    // Do nothing
  }

  @Override
  public Template getUpperTemplate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSchemaTemplate(Template schemaTemplate) {}

  @Override
  public Template getSchemaTemplate() {
    throw new RuntimeException(
        String.format("current node %s is a MeasurementMNode, can not get Device Template", name));
  }

  @Override
  public void setUseTemplate(boolean useTemplate) {}

  @Override
  public boolean isMeasurement() {
    return true;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public byte[] getRocksDBValue() throws IOException {
    return RSchemaUtils.buildMeasurementNodeValue(schema, alias, tags, attributes);
  }
}
