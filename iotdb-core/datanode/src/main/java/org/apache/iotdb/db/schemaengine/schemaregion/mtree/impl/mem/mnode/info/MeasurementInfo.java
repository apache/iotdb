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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info;

import org.apache.iotdb.commons.schema.node.info.IMeasurementInfo;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.IMeasurementSchema;

public class MeasurementInfo implements IMeasurementInfo {

  /** alias name of this measurement */
  protected String alias;

  /** tag/attribute's start offset in tag file */
  private long offset = -1;

  /** measurement's Schema for one timeseries represented by current leaf node */
  private IMeasurementSchema schema;

  /** whether this measurement is pre deleted and considered in black list */
  private boolean preDeleted = false;

  /** whether this measurement is pre altered */
  private boolean preAltered = false;

  // alias length, hashCode and occupation in aliasMap, 4 + 4 + 44 = 52B
  private static final int ALIAS_BASE_SIZE = 52;

  public MeasurementInfo(IMeasurementSchema schema, String alias) {
    this.schema = schema;
    this.alias = alias;
  }

  @Override
  public void moveDataToNewMNode(IMeasurementMNode<?> newMNode) {
    newMNode.setSchema(schema);
    newMNode.setAlias(alias);
    newMNode.setOffset(offset);
    newMNode.setPreDeleted(preDeleted);
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
  public TSDataType getDataType() {
    return schema.getType();
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
  public long getOffset() {
    return offset;
  }

  @Override
  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public boolean isPreDeleted() {
    return preDeleted;
  }

  @Override
  public void setPreDeleted(boolean preDeleted) {
    this.preDeleted = preDeleted;
  }

  /**
   * The memory occupied by an MeasurementInfo based occupation
   *
   * <ol>
   *   <li>object header, 8B
   *   <li>alias reference, 8B
   *   <li>long tagOffset, 8B
   *   <li>boolean preDeleted, 1B
   *   <li>estimated schema size, 32B
   * </ol>
   */
  @Override
  public int estimateSize() {
    return 8 + 8 + 8 + 1 + 32 + (alias == null ? 0 : ALIAS_BASE_SIZE + alias.length());
  }
}
