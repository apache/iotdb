/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.utils.cte.CteDataReader;
import org.apache.iotdb.db.utils.cte.CteDataStore;
import org.apache.iotdb.db.utils.cte.MemoryReader;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class CteScanOperator implements SourceOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(CteScanOperator.class);
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CteScanOperator.class);

  private final long maxReturnSize =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private final OperatorContext operatorContext;
  private final PlanNodeId sourceId;
  private final CteDataReader dataReader;

  public CteScanOperator(
      OperatorContext operatorContext,
      PlanNodeId sourceId,
      CteDataStore dataStore,
      QueryId queryId) {
    requireNonNull(dataStore, "dataStore is null");
    this.operatorContext = operatorContext;
    this.sourceId = sourceId;
    this.dataReader = new MemoryReader(dataStore, queryId);
  }

  @Override
  public TsBlock next() throws Exception {
    return dataReader.next();
  }

  @Override
  public boolean hasNext() throws Exception {
    return dataReader.hasNext();
  }

  @Override
  public void close() throws Exception {
    try {
      dataReader.close();
    } catch (Exception e) {
      LOGGER.error("Fail to close CteDataReader", e);
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return calculateRetainedSizeAfterCallingNext() + calculateMaxReturnSize();
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + dataReader.ramBytesUsed();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }
}
