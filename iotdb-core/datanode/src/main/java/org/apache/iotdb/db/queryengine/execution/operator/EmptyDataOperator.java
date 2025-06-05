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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

public class EmptyDataOperator implements Operator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(EmptyDataOperator.class);

  private final OperatorContext operatorContext;

  public EmptyDataOperator(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    return null;
  }

  @Override
  public boolean hasNext() throws Exception {
    return false;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public boolean isFinished() throws Exception {
    return true;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return 0;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }
}
