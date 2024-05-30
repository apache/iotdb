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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.List;

public abstract class AbstractRegionScanDataSourceOperator extends AbstractSourceOperator
    implements DataSourceOperator {

  protected boolean finished = false;

  protected AbstractRegionScanForActiveDataUtil regionScanUtil;
  protected TsBlockBuilder resultTsBlockBuilder;

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    regionScanUtil.initQueryDataSource(dataSource);
    resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    if (resultTsBlockBuilder.isEmpty()) {
      return null;
    }
    resultTsBlock = resultTsBlockBuilder.build();
    resultTsBlockBuilder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  @Override
  public void close() throws Exception {
    // do nothing
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(
        maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte() * 3L);
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemoryWithCounter() - calculateMaxReturnSize();
  }

  @Override
  public long ramBytesUsed() {
    return (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes())
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(regionScanUtil);
  }

  protected abstract List<TSDataType> getResultDataTypes();
}
