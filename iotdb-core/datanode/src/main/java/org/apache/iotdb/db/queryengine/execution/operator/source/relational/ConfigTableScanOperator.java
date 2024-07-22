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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractSourceOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ConfigTableMetaData;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

public class ConfigTableScanOperator extends AbstractSourceOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ConfigTableScanOperator.class);

  private final String tableName;

  private boolean hasNext = true;

  public ConfigTableScanOperator(
      final OperatorContext context, final PlanNodeId sourceId, final String tableName) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.tableName = tableName;
  }

  @Override
  public TsBlock next() throws Exception {
    hasNext = false;
    return ConfigTableMetaData.getTsBlock(tableName);
  }

  @Override
  public boolean hasNext() throws Exception {
    return hasNext;
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNext;
  }

  @Override
  public long calculateMaxPeekMemory() {
    // Unknown
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    // Unknown
    return 0;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE;
  }
}
