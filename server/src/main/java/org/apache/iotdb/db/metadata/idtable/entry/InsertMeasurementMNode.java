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

package org.apache.iotdb.db.metadata.idtable.entry;

import org.apache.iotdb.db.engine.trigger.executor.TriggerExecutor;
import org.apache.iotdb.db.metadata.lastCache.container.ILastCacheContainer;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.util.Map;

/**
 * Generated entity implements IMeasurementMNode interface to unify insert logic through id table
 * and mmanager
 */
public class InsertMeasurementMNode implements IMeasurementMNode {
  SchemaEntry schemaEntry;

  TriggerExecutor triggerExecutor;

  IMeasurementSchema schema;

  public InsertMeasurementMNode(String measurementId, SchemaEntry schemaEntry) {
    this(measurementId, schemaEntry, null);
  }

  public InsertMeasurementMNode(
      String measurementId, SchemaEntry schemaEntry, TriggerExecutor executor) {
    this.schemaEntry = schemaEntry;
    schema =
        new UnaryMeasurementSchema(
            measurementId,
            schemaEntry.getTSDataType(),
            schemaEntry.getTSEncoding(),
            schemaEntry.getCompressionType());
    triggerExecutor = executor;
  }

  // region support methods
  @Override
  public TriggerExecutor getTriggerExecutor() {
    return triggerExecutor;
  }

  @Override
  public ILastCacheContainer getLastCacheContainer() {
    return schemaEntry;
  }

  @Override
  public void setLastCacheContainer(ILastCacheContainer lastCacheContainer) {}

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public TSDataType getDataType(String measurementId) {
    return schemaEntry.getTSDataType();
  }

  @Override
  public IEntityMNode getParent() {
    return null;
  }

  @Override
  public String toString() {
    return schema.getMeasurementId();
  }

  // endregion

  // region unsupported methods
  @Override
  public String getName() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void setName(String name) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void setParent(IMNode parent) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public String getFullPath() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void setFullPath(String fullPath) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public PartialPath getPartialPath() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public boolean hasChild(String name) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public IMNode getChild(String name) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void addChild(String name, IMNode child) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public IMNode addChild(IMNode child) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void deleteChild(String name) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void replaceChild(String oldChildName, IMNode newChildNode) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public Map<String, IMNode> getChildren() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void setChildren(Map<String, IMNode> children) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public boolean isUseTemplate() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void setUseTemplate(boolean useTemplate) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public Template getUpperTemplate() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public Template getSchemaTemplate() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void setSchemaTemplate(Template schemaTemplate) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public boolean isEmptyInternal() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public boolean isStorageGroup() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public boolean isEntity() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public boolean isMeasurement() {
    return true;
  }

  @Override
  public IStorageGroupMNode getAsStorageGroupMNode() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public IEntityMNode getAsEntityMNode() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public IMeasurementMNode getAsMeasurementMNode() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void serializeTo(MLogWriter logWriter) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public MeasurementPath getMeasurementPath() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public String getAlias() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void setAlias(String alias) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public long getOffset() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void setOffset(long offset) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void setTriggerExecutor(TriggerExecutor triggerExecutor) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }
  // endregion
}
