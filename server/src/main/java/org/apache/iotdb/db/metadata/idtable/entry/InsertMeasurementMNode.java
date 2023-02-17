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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeType;
import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.visitor.MNodeVisitor;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheEntry;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * Generated entity implements IMeasurementMNode interface to unify insert logic through id table
 * and SchemaProcessor
 */
public class InsertMeasurementMNode implements IMeasurementMNode {
  SchemaEntry schemaEntry;

  IMeasurementSchema schema;

  public InsertMeasurementMNode(String measurementId, SchemaEntry schemaEntry) {
    this.schemaEntry = schemaEntry;
    schema =
        new MeasurementSchema(
            measurementId,
            schemaEntry.getTSDataType(),
            schemaEntry.getTSEncoding(),
            schemaEntry.getCompressionType());
  }

  // region support methods

  @Override
  public boolean isPreDeleted() {
    return false;
  }

  @Override
  public void setPreDeleted(boolean preDeleted) {}

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
  public IMNode addChild(String name, IMNode child) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public IMNode addChild(IMNode child) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public IMNode deleteChild(String name) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void replaceChild(String oldChildName, IMNode newChildNode) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void moveDataToNewMNode(IMNode newMNode) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public IMNodeContainer getChildren() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void setChildren(IMNodeContainer children) {
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
  public int getSchemaTemplateId() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public int getSchemaTemplateIdWithState() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void setSchemaTemplateId(int schemaTemplateId) {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void preUnsetSchemaTemplate() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void rollbackUnsetSchemaTemplate() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public boolean isSchemaTemplatePreUnset() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public void unsetSchemaTemplate() {
    throw new UnsupportedOperationException("insert measurement mnode doesn't support this method");
  }

  @Override
  public boolean isAboveDatabase() {
    return false;
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
  public MNodeType getMNodeType(Boolean isConfig) {
    return MNodeType.MEASUREMENT;
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
  public CacheEntry getCacheEntry() {
    return null;
  }

  @Override
  public void setCacheEntry(CacheEntry cacheEntry) {}

  @Override
  public <R, C> R accept(MNodeVisitor<R, C> visitor, C context) {
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

  // endregion
}
