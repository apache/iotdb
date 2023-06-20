/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.query.info.ISchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.schema.source.ISchemaSource;
import org.apache.iotdb.db.mpp.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class CountGroupByLevelScanOperator<T extends ISchemaInfo> implements SourceOperator {

  private static final int DEFAULT_BATCH_SIZE = 1000;

  private static final List<TSDataType> OUTPUT_DATA_TYPES =
      ImmutableList.of(TSDataType.TEXT, TSDataType.INT64);

  private final PlanNodeId sourceId;
  private final OperatorContext operatorContext;

  private final int level;

  private final ISchemaSource<T> schemaSource;

  private ISchemaReader<T> schemaReader;

  private ListenableFuture<Boolean> blockedHasNext;
  private Map<PartialPath, Long> countMap;
  private boolean isFinished;

  public CountGroupByLevelScanOperator(
      PlanNodeId sourceId,
      OperatorContext operatorContext,
      int level,
      ISchemaSource<T> schemaSource) {
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.level = level;
    this.schemaSource = schemaSource;
    this.countMap = new HashMap<>();
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return blockedHasNext == null ? NOT_BLOCKED : blockedHasNext;
  }

  @Override
  public TsBlock next() throws Exception {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    if (schemaReader == null) {
      schemaReader = createTimeSeriesReader();
    }
    while (true) {
      ListenableFuture<Boolean> hasNext = schemaReader.hasNextFuture();
      if (!hasNext.isDone()) {
        blockedHasNext = hasNext;
        return null;
      } else if (hasNext.get()) {
        if (!schemaReader.isSuccess()) {
          throw new RuntimeException(schemaReader.getFailure());
        }
        isFinished = true;
        blockedHasNext = null;
        return constructTsBlockAndClearMap(countMap);
      }
      ISchemaInfo schemaInfo = schemaReader.next();
      PartialPath path = schemaInfo.getPartialPath();
      if (path.getNodeLength() < level) {
        continue;
      }
      PartialPath levelPath = new PartialPath(Arrays.copyOf(path.getNodes(), level + 1));
      countMap.compute(
          levelPath,
          (k, v) -> {
            if (v == null) {
              return 1L;
            } else {
              return v + 1;
            }
          });
      if (countMap.size() == DEFAULT_BATCH_SIZE) {
        blockedHasNext = null;
        return constructTsBlockAndClearMap(countMap);
      }
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    return !isFinished;
  }

  public ISchemaReader<T> createTimeSeriesReader() {
    return schemaSource.getSchemaReader(
        ((SchemaDriverContext) operatorContext.getDriverContext()).getSchemaRegion());
  }

  private TsBlock constructTsBlockAndClearMap(Map<PartialPath, Long> countMap) {
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(OUTPUT_DATA_TYPES);
    for (Map.Entry<PartialPath, Long> entry : countMap.entrySet()) {
      tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
      tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(entry.getKey().getFullPath()));
      tsBlockBuilder.getColumnBuilder(1).writeLong(entry.getValue());
      tsBlockBuilder.declarePosition();
    }
    countMap.clear();
    return tsBlockBuilder.build();
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateMaxReturnSize() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  @Override
  public void close() throws Exception {
    if (schemaReader != null) {
      schemaReader.close();
    }
  }
}
