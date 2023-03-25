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
package org.apache.iotdb.commons.schema.node.common;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.MNodeType;
import org.apache.iotdb.commons.schema.node.info.IMeasurementInfo;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMeasurementMNode<N extends IMNode<N>, BasicNode extends IMNode<N>>
    implements IMeasurementMNode<N> {

  private static final Logger logger = LoggerFactory.getLogger(AbstractMeasurementMNode.class);

  private final IMeasurementInfo measurementInfo;
  protected final BasicNode basicMNode;

  public AbstractMeasurementMNode(BasicNode basicMNode, IMeasurementInfo measurementInfo) {
    this.basicMNode = basicMNode;
    this.measurementInfo = measurementInfo;
  }

  public BasicNode getBasicMNode() {
    return basicMNode;
  }

  @Override
  public String getName() {
    return basicMNode.getName();
  }

  @Override
  public void setName(String name) {
    basicMNode.setName(name);
  }

  @Override
  public N getParent() {
    if (basicMNode.getParent() == null) {
      return null;
    }
    return basicMNode.getParent();
  }

  @Override
  public void setParent(N parent) {
    basicMNode.setParent(parent);
  }

  /**
   * get MeasurementPath of this node
   *
   * @return MeasurementPath
   */
  @Override
  public MeasurementPath getMeasurementPath() {
    MeasurementPath result = new MeasurementPath(getPartialPath(), getSchema());
    result.setUnderAlignedEntity(getParent().getAsDeviceMNode().isAligned());
    return result;
  }

  @Override
  public IMeasurementSchema getSchema() {
    return measurementInfo.getSchema();
  }

  @Override
  public void setSchema(IMeasurementSchema schema) {
    measurementInfo.setSchema(schema);
  }

  @Override
  public TSDataType getDataType() {
    return measurementInfo.getDataType();
  }

  @Override
  public long getOffset() {
    return measurementInfo.getOffset();
  }

  @Override
  public void setOffset(long offset) {
    measurementInfo.setOffset(offset);
  }

  @Override
  public String getAlias() {
    return measurementInfo.getAlias();
  }

  @Override
  public void setAlias(String alias) {
    measurementInfo.setAlias(alias);
  }

  @Override
  public boolean isPreDeleted() {
    return measurementInfo.isPreDeleted();
  }

  @Override
  public void setPreDeleted(boolean preDeleted) {
    measurementInfo.setPreDeleted(preDeleted);
  }

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
    return visitor.visitMeasurementMNode(this, context);
  }

  @Override
  public String getFullPath() {
    return basicMNode.getFullPath();
  }

  @Override
  public void setFullPath(String fullPath) {
    basicMNode.setFullPath(fullPath);
  }

  @Override
  public PartialPath getPartialPath() {
    return basicMNode.getPartialPath();
  }

  @Override
  public boolean hasChild(String name) {
    return false;
  }

  @Override
  public N getChild(String name) {
    logger.warn("current node {} is a MeasurementMNode, can not get child {}", getName(), name);
    throw new RuntimeException(
        String.format(
            "current node %s is a MeasurementMNode, can not get child %s", getName(), name));
  }

  @Override
  public N addChild(String name, N child) {
    // Do nothing
    return null;
  }

  @Override
  public N addChild(N child) {
    return null;
  }

  @Override
  public N deleteChild(String name) {
    return null;
  }

  @Override
  public void replaceChild(String oldChildName, N newChildNode) {}

  @Override
  public void moveDataToNewMNode(N newMNode) {
    basicMNode.moveDataToNewMNode(newMNode);
    if (newMNode.isMeasurement()) {
      measurementInfo.moveDataToNewMNode(newMNode.getAsMeasurementMNode());
    }
  }

  @Override
  public void setChildren(IMNodeContainer<N> children) {
    // Do nothing
  }

  @Override
  public boolean isAboveDatabase() {
    return false;
  }

  @Override
  public boolean isDatabase() {
    return false;
  }

  @Override
  public boolean isDevice() {
    return false;
  }

  @Override
  public boolean isMeasurement() {
    return true;
  }

  @Override
  public MNodeType getMNodeType(Boolean isConfig) {
    return MNodeType.MEASUREMENT;
  }

  @Override
  public IDatabaseMNode<N> getAsDatabaseMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public IDeviceMNode<N> getAsDeviceMNode() {
    throw new UnsupportedOperationException("Wrong MNode Type");
  }

  @Override
  public IMeasurementMNode<N> getAsMeasurementMNode() {
    return this;
  }

  /**
   * The basic memory occupied by any AbstractMeasurementMNode object
   *
   * <ol>
   *   <li>object header, 8B
   *   <li>node attributes
   *       <ol>
   *         <li>measurementInfo reference, 8B
   *         <li>basicMNode reference, 8B
   *       </ol>
   *   <li>MapEntry in parent
   * </ol>
   */
  @Override
  public int estimateSize() {
    return 8 + 8 + measurementInfo.estimateSize() + basicMNode.estimateSize();
  }
}
