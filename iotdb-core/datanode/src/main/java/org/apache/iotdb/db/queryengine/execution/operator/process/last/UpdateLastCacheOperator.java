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

package org.apache.iotdb.db.queryengine.execution.operator.process.last;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;

public class UpdateLastCacheOperator extends AbstractUpdateLastCacheOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UpdateLastCacheOperator.class);

  // fullPath for queried time series
  // It should be exact PartialPath, neither MeasurementPath nor AlignedPath, because lastCache only
  // accept PartialPath
  private final MeasurementPath fullPath;

  // type for queried time series
  protected final String dataType;

  public UpdateLastCacheOperator(
      OperatorContext operatorContext,
      Operator child,
      MeasurementPath fullPath,
      TSDataType dataType,
      TreeDeviceSchemaCacheManager treeDeviceSchemaCacheManager,
      boolean needUpdateCache,
      boolean isNeedUpdateNullEntry) {
    super(
        operatorContext,
        child,
        treeDeviceSchemaCacheManager,
        needUpdateCache,
        isNeedUpdateNullEntry);
    this.fullPath = fullPath;
    this.dataType = dataType.name();
  }

  @Override
  public TsBlock next() throws Exception {
    TsBlock res = child.nextWithTimer();
    if (res == null) {
      return null;
    }
    if (res.isEmpty()) {
      return LAST_QUERY_EMPTY_TSBLOCK;
    }

    checkArgument(res.getPositionCount() == 1, "last query result should only have one record");

    // last value is null
    if (res.getColumn(0).isNull(0)) {
      // we still need to update last cache if there is no data for this time series to avoid
      // scanning all files each time
      mayUpdateLastCache(Long.MIN_VALUE, null, fullPath);
      return LAST_QUERY_EMPTY_TSBLOCK;
    }

    long lastTime = res.getColumn(0).getLong(0);
    TsPrimitiveType lastValue = res.getColumn(1).getTsPrimitiveType(0);

    mayUpdateLastCache(lastTime, lastValue, fullPath);

    tsBlockBuilder.reset();
    appendLastValueToTsBlockBuilder(lastTime, lastValue);
    return tsBlockBuilder.build();
  }

  protected void appendLastValueToTsBlockBuilder(long lastTime, TsPrimitiveType lastValue) {
    LastQueryUtil.appendLastValue(
        tsBlockBuilder, lastTime, fullPath.getFullPath(), lastValue.getStringValue(), dataType);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + RamUsageEstimator.sizeOf(databaseName)
        + RamUsageEstimator.sizeOf(dataType)
        + MemoryEstimationHelper.getEstimatedSizeOfPartialPath(fullPath)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
