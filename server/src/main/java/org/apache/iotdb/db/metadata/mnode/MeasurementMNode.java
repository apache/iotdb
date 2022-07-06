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
import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.container.MNodeContainers;
import org.apache.iotdb.db.metadata.mnode.visitor.MNodeVisitor;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.qp.physical.sys.MeasurementMNodePlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MeasurementMNode extends MNode implements IMeasurementMNode {

  private static final Logger logger = LoggerFactory.getLogger(MeasurementMNode.class);

  /** alias name of this measurement */
  protected String alias;
  /** tag/attribute's start offset in tag file */
  private long offset = -1;
  /** measurement's Schema for one timeseries represented by current leaf node */
  private IMeasurementSchema schema;
  /** last value cache */
  private volatile ILastCacheContainer lastCacheContainer = null;

  /**
   * MeasurementMNode factory method. The type of returned MeasurementMNode is according to the
   * schema type. The default type is UnaryMeasurementMNode, which means if schema == null, an
   * UnaryMeasurementMNode will return.
   */
  public static IMeasurementMNode getMeasurementMNode(
      IEntityMNode parent, String measurementName, IMeasurementSchema schema, String alias) {
    return new MeasurementMNode(parent, measurementName, schema, alias);
  }

  /** @param alias alias of measurementName */
  public MeasurementMNode(IMNode parent, String name, IMeasurementSchema schema, String alias) {
    super(parent, name);
    this.schema = schema;
    this.alias = alias;
  }

  @Override
  public IEntityMNode getParent() {
    if (parent == null) {
      return null;
    }
    return parent.getAsEntityMNode();
  }

  /**
   * get MeasurementPath of this node
   *
   * @return MeasurementPath
   */
  @Override
  public MeasurementPath getMeasurementPath() {
    MeasurementPath result = new MeasurementPath(super.getPartialPath(), schema);
    result.setUnderAlignedEntity(getParent().isAligned());
    return result;
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  /**
   * get data type
   *
   * @param measurementId if it's a vector schema, we need sensor name of it
   * @return measurement data type
   */
  @Override
  public TSDataType getDataType(String measurementId) {
    return schema.getType();
  }

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

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    return visitor.visitMeasurementMNode(this, context);
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
  public IMNode addChild(String name, IMNode child) {
    // Do nothing
    return null;
  }

  @Override
  public IMNode addChild(IMNode child) {
    return null;
  }

  @Override
  public IMNode deleteChild(String name) {
    return null;
  }

  @Override
  public void replaceChild(String oldChildName, IMNode newChildNode) {}

  @Override
  public IMNodeContainer getChildren() {
    return MNodeContainers.emptyMNodeContainer();
  }

  @Override
  public void setChildren(IMNodeContainer children) {
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
  public void setUseTemplate(boolean useTemplate) {}

  @Override
  public boolean isMeasurement() {
    return true;
  }
}
