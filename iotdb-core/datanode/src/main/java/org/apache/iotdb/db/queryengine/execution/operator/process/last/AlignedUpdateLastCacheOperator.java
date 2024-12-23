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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

/** update last cache for aligned series. */
public class AlignedUpdateLastCacheOperator extends AbstractUpdateLastCacheOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AlignedUpdateLastCacheOperator.class);

  private final AlignedPath seriesPath;

  private final PartialPath devicePath;

  public AlignedUpdateLastCacheOperator(
      OperatorContext operatorContext,
      Operator child,
      AlignedPath seriesPath,
      TreeDeviceSchemaCacheManager treeDeviceSchemaCacheManager,
      boolean needUpdateCache,
      boolean needUpdateNullEntry) {
    super(
        operatorContext, child, treeDeviceSchemaCacheManager, needUpdateCache, needUpdateNullEntry);
    this.seriesPath = seriesPath;
    this.devicePath = seriesPath.getDevicePath();
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

    if (res.getPositionCount() != 1) {
      throw new IllegalArgumentException("last read result should only have one record");
    }

    tsBlockBuilder.reset();
    for (int i = 0; i + 1 < res.getValueColumnCount(); i += 2) {
      MeasurementPath measurementPath =
          new MeasurementPath(
              devicePath.concatNode(seriesPath.getMeasurementList().get(i / 2)),
              seriesPath.getSchemaList().get(i / 2),
              true);
      if (!res.getColumn(i).isNull(0)) {
        long lastTime = res.getColumn(i).getLong(0);
        TsPrimitiveType lastValue = res.getColumn(i + 1).getTsPrimitiveType(0);
        mayUpdateLastCache(lastTime, lastValue, measurementPath);
        appendLastValueToTsBlockBuilder(
            lastTime,
            lastValue,
            measurementPath,
            seriesPath.getSchemaList().get(i / 2).getType().name());
      } else {
        // we still need to update last cache if there is no data for this time series to avoid
        // scanning all files each time
        mayUpdateLastCache(Long.MIN_VALUE, null, measurementPath);
      }
    }
    return !tsBlockBuilder.isEmpty() ? tsBlockBuilder.build() : LAST_QUERY_EMPTY_TSBLOCK;
  }

  protected void appendLastValueToTsBlockBuilder(
      long lastTime, TsPrimitiveType lastValue, MeasurementPath measurementPath, String type) {
    LastQueryUtil.appendLastValue(
        tsBlockBuilder, lastTime, measurementPath.getFullPath(), lastValue.getStringValue(), type);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + RamUsageEstimator.sizeOf(databaseName)
        + MemoryEstimationHelper.getEstimatedSizeOfPartialPath(devicePath)
        + MemoryEstimationHelper.getEstimatedSizeOfPartialPath(seriesPath)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
