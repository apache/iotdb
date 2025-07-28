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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

abstract class AbstractTableDevicesDeletionNode extends PlanNode implements ISchemaRegionPlan {

  protected final String tableName;
  protected final byte[] patternInfo;

  protected AbstractTableDevicesDeletionNode(
      final PlanNodeId id, final String tableName, final @Nonnull byte[] patternInfo) {
    super(id);
    this.tableName = tableName;
    this.patternInfo = patternInfo;
  }

  public String getTableName() {
    return tableName;
  }

  public byte[] getPatternInfo() {
    return patternInfo;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(final PlanNode child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    ReadWriteIOUtils.write(tableName, byteBuffer);
    ReadWriteIOUtils.write(patternInfo.length, byteBuffer);
    byteBuffer.put(patternInfo);
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(patternInfo.length, stream);
    stream.write(patternInfo);
  }
}
