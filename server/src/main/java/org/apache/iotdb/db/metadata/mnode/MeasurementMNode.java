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
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Represents an MNode which has a Measurement or Sensor attached to it. */
public class MeasurementMNode extends MNode implements IMeasurementMNode {

  private static final Logger logger = LoggerFactory.getLogger(MeasurementMNode.class);

  private static final long serialVersionUID = -1199657856921206435L;

  /** measurement's Schema for one timeseries represented by current leaf node */
  private IMeasurementSchema schema;

  /** alias name of this measurement */
  private String alias;

  /** tag/attribute's start offset in tag file */
  private long offset = -1;

  /** last value cache */
  private volatile ILastCacheContainer lastCacheContainer = null;

  /** registered trigger */
  private TriggerExecutor triggerExecutor = null;

  /** @param alias alias of measurementName */
  public MeasurementMNode(
      IMNode parent,
      String measurementName,
      String alias,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType type,
      Map<String, String> props) {
    super(parent, measurementName);
    this.schema = new MeasurementSchema(measurementName, dataType, encoding, type, props);
    this.alias = alias;
  }

  public MeasurementMNode(
      IMNode parent, String measurementName, IMeasurementSchema schema, String alias) {
    super(parent, measurementName);
    this.schema = schema;
    this.alias = alias;
  }

  @Override
  public IEntityMNode getParent() {
    return (IEntityMNode) parent;
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public void setSchema(IMeasurementSchema schema) {
    this.schema = schema;
  }

  @Override
  public int getMeasurementMNodeCount() {
    return 1;
  }

  @Override
  public int getMeasurementCount() {
    return schema.getSubMeasurementsCount();
  }

  /**
   * get data type
   *
   * @param measurementId if it's a vector schema, we need sensor name of it
   * @return measurement data type
   */
  @Override
  public TSDataType getDataType(String measurementId) {
    if (schema instanceof MeasurementSchema) {
      return schema.getType();
    } else {
      int index = schema.getSubMeasurementIndex(measurementId);
      return schema.getSubMeasurementsTSDataTypeList().get(index);
    }
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

  /**
   * deserialize MeasuremetMNode from string array
   *
   * @param nodeInfo node information array. For example:
   *     "2,s0,speed,2,2,1,year:2020;month:jan;,-1,0" representing: [0] nodeType [1] name [2] alias
   *     [3] TSDataType.ordinal() [4] TSEncoding.ordinal() [5] CompressionType.ordinal() [6] props
   *     [7] offset [8] children size
   */
  public static IMeasurementMNode deserializeFrom(String[] nodeInfo) {
    String name = nodeInfo[1];
    String alias = nodeInfo[2].equals("") ? null : nodeInfo[2];
    Map<String, String> props = new HashMap<>();
    if (!nodeInfo[6].equals("")) {
      for (String propInfo : nodeInfo[6].split(";")) {
        props.put(propInfo.split(":")[0], propInfo.split(":")[1]);
      }
    }
    IMeasurementSchema schema =
        new MeasurementSchema(
            name,
            Byte.parseByte(nodeInfo[3]),
            Byte.parseByte(nodeInfo[4]),
            Byte.parseByte(nodeInfo[5]),
            props);
    IMeasurementMNode node = new MeasurementMNode(null, name, schema, alias);
    node.setOffset(Long.parseLong(nodeInfo[7]));
    return node;
  }

  /** deserialize MeasuremetMNode from MeasurementNodePlan */
  public static IMeasurementMNode deserializeFrom(MeasurementMNodePlan plan) {
    IMeasurementMNode node =
        new MeasurementMNode(null, plan.getName(), plan.getSchema(), plan.getAlias());
    node.setOffset(plan.getOffset());

    return node;
  }

  @Override
  public String getFullPath() {
    return concatFullPath();
  }

  @Override
  public boolean hasChild(String name) {
    return false;
  }

  @Override
  public IMNode getChild(String name) {
    logger.warn("current node {} is a MeasurementMNode, can not get child {}", super.name, name);
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
    logger.warn("current node {} is a MeasurementMNode, can not get Device Template", name);
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
