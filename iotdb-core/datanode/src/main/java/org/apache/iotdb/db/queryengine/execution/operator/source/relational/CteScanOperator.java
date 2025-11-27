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

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractSourceOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.utils.cte.CteDataReader;
import org.apache.iotdb.db.utils.cte.CteDataStore;
import org.apache.iotdb.db.utils.cte.MemoryReader;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CteScanOperator extends AbstractSourceOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(CteScanOperator.class);
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CteScanOperator.class);

  private final CteDataStore dataStore;
  private CteDataReader dataReader;

  private final long maxReturnSize =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  public CteScanOperator(
      OperatorContext operatorContext, PlanNodeId sourceId, CteDataStore dataStore) {
    this.operatorContext = operatorContext;
    this.sourceId = sourceId;
    dataStore.increaseRefCount();
    this.dataStore = dataStore;
    prepareReader();
  }

  @Override
  public TsBlock next() throws Exception {
    if (dataReader == null) {
      return null;
    }
    return dataReader.next();
  }

  @Override
  public boolean hasNext() throws Exception {
    if (dataReader == null) {
      return false;
    }
    return dataReader.hasNext();
  }

  @Override
  public void close() throws Exception {
    try {
      if (dataReader != null) {
        dataReader.close();
      }
    } catch (Exception e) {
      LOGGER.error("Fail to close fileChannel", e);
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
    long bytes =
        INSTANCE_SIZE + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
    if (dataReader != null) {
      bytes += dataReader.bytesUsed();
    }
    if (dataStore.getRefCount() == 1) {
      bytes += dataStore.getCachedBytes();
    }

    return bytes;
  }

  private void prepareReader() {
    if (dataStore.getCachedBytes() != 0) {
      dataReader = new MemoryReader(dataStore.getCachedData());
    }
  }
}
