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
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractUpdateLastCacheOperator implements ProcessOperator {
  protected static final TsBlock LAST_QUERY_EMPTY_TSBLOCK =
      new TsBlockBuilder(ImmutableList.of(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT))
          .build();

  protected OperatorContext operatorContext;

  protected final DataNodeQueryContext dataNodeQueryContext;

  protected Operator child;

  private final TreeDeviceSchemaCacheManager lastCache;

  private final boolean needUpdateCache;

  private final boolean needUpdateNullEntry;

  protected TsBlockBuilder tsBlockBuilder;

  protected String databaseName;

  protected AbstractUpdateLastCacheOperator(
      final OperatorContext operatorContext,
      final Operator child,
      final TreeDeviceSchemaCacheManager treeDeviceSchemaCacheManager,
      final boolean needUpdateCache,
      final boolean needUpdateNullEntry) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.lastCache = treeDeviceSchemaCacheManager;
    this.needUpdateCache = needUpdateCache;
    this.needUpdateNullEntry = needUpdateNullEntry;
    this.tsBlockBuilder = LastQueryUtil.createTsBlockBuilder(1);
    this.dataNodeQueryContext =
        operatorContext.getDriverContext().getFragmentInstanceContext().getDataNodeQueryContext();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  protected String getDatabaseName() {
    if (databaseName == null) {
      databaseName =
          ((DataDriverContext) operatorContext.getDriverContext())
              .getDataRegion()
              .getDatabaseName();
    }
    return databaseName;
  }

  protected void mayUpdateLastCache(
      final long time, final @Nullable TsPrimitiveType value, final MeasurementPath fullPath) {
    if (!needUpdateCache) {
      return;
    }
    try {
      dataNodeQueryContext.lock();
      final Pair<AtomicInteger, TimeValuePair> seriesScanInfo =
          dataNodeQueryContext.getSeriesScanInfo(fullPath);

      // may enter this case when use TTL
      if (seriesScanInfo == null) {
        return;
      }

      // update cache in DataNodeQueryContext
      if (seriesScanInfo.right == null || time > seriesScanInfo.right.getTimestamp()) {
        if (Objects.nonNull(value)) {
          seriesScanInfo.right = new TimeValuePair(time, value);
        } else {
          seriesScanInfo.right =
              needUpdateNullEntry ? TableDeviceLastCache.EMPTY_TIME_VALUE_PAIR : null;
        }
      }

      if (seriesScanInfo.left.decrementAndGet() == 0) {
        lastCache.updateLastCacheIfExists(
            getDatabaseName(),
            fullPath.getIDeviceID(),
            new String[] {fullPath.getMeasurement()},
            new TimeValuePair[] {seriesScanInfo.right},
            fullPath.isUnderAlignedEntity(),
            new IMeasurementSchema[] {fullPath.getMeasurementSchema()});
      }
    } finally {
      dataNodeQueryContext.unLock();
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    return child.hasNextWithTimer();
  }

  @Override
  public boolean isFinished() throws Exception {
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
