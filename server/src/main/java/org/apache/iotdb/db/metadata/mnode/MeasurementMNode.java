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

import org.apache.iotdb.db.engine.trigger.executor.TriggerExecutor;
import org.apache.iotdb.db.metadata.lastCache.container.ILastCacheContainer;
import org.apache.iotdb.db.metadata.lastCache.container.LastCacheContainer;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.qp.physical.sys.MeasurementMNodePlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public abstract class MeasurementMNode extends MNode implements IMeasurementMNode {

  private static final Logger logger = LoggerFactory.getLogger(MeasurementMNode.class);

  /** alias name of this measurement */
  protected String alias;
  /** tag/attribute's start offset in tag file */
  private long offset = -1;
  /** last value cache */
  private volatile ILastCacheContainer lastCacheContainer = null;
  /** registered trigger */
  private TriggerExecutor triggerExecutor = null;

  /**
   * MeasurementMNode factory method. The type of returned MeasurementMNode is according to the
   * schema type. The default type is UnaryMeasurementMNode, which means if schema == null, an
   * UnaryMeasurementMNode will return.
   */
  public static IMeasurementMNode getMeasurementMNode(
      IEntityMNode parent, String measurementName, IMeasurementSchema schema, String alias) {
    if (schema == null) {
      return new UnaryMeasurementMNode(parent, measurementName, null, alias);
    } else if (schema instanceof UnaryMeasurementSchema) {
      return new UnaryMeasurementMNode(
          parent, measurementName, (UnaryMeasurementSchema) schema, alias);
    } else if (schema instanceof VectorMeasurementSchema) {
      return new MultiMeasurementMNode(
          parent, measurementName, (VectorMeasurementSchema) schema, alias);
    } else {
      throw new RuntimeException("Undefined schema type.");
    }
  }

  /** @param alias alias of measurementName */
  MeasurementMNode(IMNode parent, String name, String alias) {
    super(parent, name);
    this.alias = alias;
  }

  @Override
  public IEntityMNode getParent() {
    if (parent == null) {
      return null;
    }
    return parent.getAsEntityMNode();
  }

  @Override
  public abstract IMeasurementSchema getSchema();

  @Override
  public abstract int getMeasurementCount();

  /**
   * get data type
   *
   * @param measurementId if it's a vector schema, we need sensor name of it
   * @return measurement data type
   */
  @Override
  public abstract TSDataType getDataType(String measurementId);

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public void setOffset(long offset) {
    this.offset = offset;
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
    return triggerExecutor;
  }

  @Override
  public void setTriggerExecutor(TriggerExecutor triggerExecutor) {
    this.triggerExecutor = triggerExecutor;
  }

  @Override
  public ILastCacheContainer getLastCacheContainer() {
    if (lastCacheContainer == null) {
      synchronized (this) {
        if (lastCacheContainer == null) {
          lastCacheContainer = new LastCacheContainer();
        }
      }
    }
    return lastCacheContainer;
  }

  @Override
  public void setLastCacheContainer(ILastCacheContainer lastCacheContainer) {
    this.lastCacheContainer = lastCacheContainer;
  }

  @Override
  public void serializeTo(MLogWriter logWriter) throws IOException {
    logWriter.serializeMeasurementMNode(this);
  }

  /** deserialize MeasurementMNode from MeasurementNodePlan */
  public static IMeasurementMNode deserializeFrom(MeasurementMNodePlan plan) {
    IMeasurementMNode node =
        MeasurementMNode.getMeasurementMNode(
            null, plan.getName(), plan.getSchema(), plan.getAlias());
    node.setOffset(plan.getOffset());
    return node;
  }

  @Override
  public boolean isUnaryMeasurement() {
    return false;
  }

  @Override
  public boolean isMultiMeasurement() {
    return false;
  }

  @Override
  public UnaryMeasurementMNode getAsUnaryMeasurementMNode() {
    if (isUnaryMeasurement()) {
      return (UnaryMeasurementMNode) this;
    } else {
      throw new UnsupportedOperationException("This is not an UnaryMeasurementMNode");
    }
  }

  @Override
  public MultiMeasurementMNode getAsMultiMeasurementMNode() {
    if (isMultiMeasurement()) {
      return (MultiMeasurementMNode) this;
    } else {
      throw new UnsupportedOperationException("This is not an MultiMeasurementMNode");
    }
  }

  @Override
  public String getFullPath() {
    if (fullPath != null) {
      return fullPath;
    }
    return concatFullPath();
  }

  @Override
  public boolean hasChild(String name) {
    return false;
  }

  @Override
  public IMNode getChild(String name) {
    MeasurementMNode.logger.warn(
        "current node {} is a MeasurementMNode, can not get child {}", this.name, name);
    throw new RuntimeException(
        String.format(
            "current node %s is a MeasurementMNode, can not get child %s", super.name, name));
  }

  @Override
  public void addChild(String name, IMNode child) {
    // Do nothing
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
    return Collections.emptyMap();
  }

  @Override
  public void setChildren(Map<String, IMNode> children) {
    // Do nothing
  }

  @Override
  public Template getUpperTemplate() {
    return parent.getUpperTemplate();
  }

  @Override
  public Template getSchemaTemplate() {
    MeasurementMNode.logger.warn(
        "current node {} is a MeasurementMNode, can not get Device Template", name);
    throw new RuntimeException(
        String.format("current node %s is a MeasurementMNode, can not get Device Template", name));
  }

  @Override
  public void setSchemaTemplate(Template schemaTemplate) {}

  @Override
  public boolean isMeasurement() {
    return true;
  }
}
