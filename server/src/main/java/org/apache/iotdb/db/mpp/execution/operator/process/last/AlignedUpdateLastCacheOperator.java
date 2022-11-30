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

package org.apache.iotdb.db.mpp.execution.operator.process.last;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.mpp.execution.driver.DataDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.weakref.jmx.internal.guava.base.Preconditions.checkArgument;

public class AlignedUpdateLastCacheOperator extends UpdateLastCacheOperator {
  private static final TsBlock LAST_QUERY_EMPTY_TSBLOCK =
      new TsBlockBuilder(ImmutableList.of(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT))
          .build();

  private final OperatorContext operatorContext;

  private final Operator child;

  private final AlignedPath seriesPath;

  private PartialPath devicePath;

  private final DataNodeSchemaCache lastCache;

  private final boolean needUpdateCache;

  private final TsBlockBuilder tsBlockBuilder;

  private String databaseName;

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AlignedUpdateLastCacheOperator.class);

  public AlignedUpdateLastCacheOperator(
      OperatorContext operatorContext,
      Operator child,
      AlignedPath seriesPath,
      DataNodeSchemaCache dataNodeSchemaCache,
      boolean needUpdateCache) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.seriesPath = seriesPath;
    this.devicePath = seriesPath.getDevicePath();
    this.lastCache = dataNodeSchemaCache;
    this.needUpdateCache = needUpdateCache;
    this.tsBlockBuilder = LastQueryUtil.createTsBlockBuilder(1);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() {
    TsBlock res = child.next();
    if (res == null) {
      return null;
    }
    if (res.isEmpty()) {
      return LAST_QUERY_EMPTY_TSBLOCK;
    }

    checkArgument(res.getPositionCount() == 1, "last query result should only have one record");

    tsBlockBuilder.reset();
    boolean hasNonNullLastValue = false;
    for (int i = 0; i + 1 < res.getValueColumnCount(); i += 2) {
      if (!res.getColumn(i).isNull(0)) {
        hasNonNullLastValue = true;
        long lastTime = res.getColumn(i).getLong(0);
        TsPrimitiveType lastValue = res.getColumn(i + 1).getTsPrimitiveType(0);
        MeasurementPath measurementPath =
            new MeasurementPath(
                devicePath.concatNode(seriesPath.getMeasurementList().get(i / 2)),
                seriesPath.getSchemaList().get(i / 2),
                true);
        if (needUpdateCache) {
          TimeValuePair timeValuePair = new TimeValuePair(lastTime, lastValue);
          lastCache.updateLastCache(
              getDatabaseName(), measurementPath, timeValuePair, false, Long.MIN_VALUE);
        }
        LOGGER.warn("MeasurementPath in operator: {}", measurementPath.getFullPath());
        LastQueryUtil.appendLastValue(
            tsBlockBuilder,
            lastTime,
            measurementPath.getFullPath(),
            lastValue.getStringValue(),
            seriesPath.getSchemaList().get(i / 2).getType().name());
      }
    }

    return hasNonNullLastValue ? tsBlockBuilder.build() : LAST_QUERY_EMPTY_TSBLOCK;
  }

  private String getDatabaseName() {
    if (databaseName == null) {
      databaseName =
          ((DataDriverContext) operatorContext.getInstanceContext().getDriverContext())
              .getDataRegion()
              .getStorageGroupName();
    }
    return databaseName;
  }

  @Override
  public boolean hasNext() {
    return child.hasNext();
  }

  @Override
  public boolean isFinished() {
    return child.isFinished();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext();
  }
}
