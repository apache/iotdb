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

package org.apache.iotdb.db.queryengine.execution.operator.schema;

import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.ISchemaSource;
import org.apache.iotdb.db.queryengine.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ISchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

public class SchemaCountOperator<T extends ISchemaInfo> implements SourceOperator {

  private static final List<TSDataType> OUTPUT_DATA_TYPES =
      Collections.singletonList(TSDataType.INT64);

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SchemaCountOperator.class);

  private final PlanNodeId sourceId;
  private final OperatorContext operatorContext;

  private final ISchemaSource<T> schemaSource;

  private ISchemaReader<T> schemaReader;
  private int count;
  private boolean isFinished;
  private ListenableFuture<?> isBlocked;
  private TsBlock next; // next will be set only when done

  public SchemaCountOperator(
      final PlanNodeId sourceId,
      final OperatorContext operatorContext,
      final ISchemaSource<T> schemaSource) {
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.schemaSource = schemaSource;
  }

  private ISchemaRegion getSchemaRegion() {
    return ((SchemaDriverContext) operatorContext.getDriverContext()).getSchemaRegion();
  }

  private ISchemaReader<T> createSchemaReader() {
    return schemaSource.getSchemaReader(getSchemaRegion());
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (isBlocked == null) {
      isBlocked = tryGetNext();
    }
    return isBlocked;
  }

  /**
   * Try to get next TsBlock. If the next is not ready, return a future. After success, {@link
   * SchemaCountOperator#next} will be set.
   */
  private ListenableFuture<?> tryGetNext() {
    ISchemaRegion schemaRegion = getSchemaRegion();
    if (schemaSource.hasSchemaStatistic(schemaRegion)) {
      next = constructTsBlock(schemaSource.getSchemaStatistic(schemaRegion));
      return NOT_BLOCKED;
    } else {
      if (schemaReader == null) {
        schemaReader = createSchemaReader();
      }
      while (true) {
        try {
          ListenableFuture<?> readerBlocked = schemaReader.isBlocked();
          if (!readerBlocked.isDone()) {
            return readerBlocked;
          } else if (schemaReader.hasNext()) {
            schemaReader.next();
            count++;
          } else {
            next = constructTsBlock(count);
            return NOT_BLOCKED;
          }
        } catch (Exception e) {
          throw new SchemaExecutionException(e);
        }
      }
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    if (next != null) {
      isFinished = true;
    }
    isBlocked = null;
    return next;
  }

  @Override
  public boolean hasNext() throws Exception {
    isBlocked().get(); // wait for the next TsBlock
    if (schemaReader != null && !schemaReader.isSuccess()) {
      throw new SchemaExecutionException(schemaReader.getFailure());
    }
    return !isFinished;
  }

  private TsBlock constructTsBlock(long count) {
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(OUTPUT_DATA_TYPES);
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeLong(count);
    tsBlockBuilder.declarePosition();
    return tsBlockBuilder.build();
  }

  @Override
  public boolean isFinished() throws Exception {
    return isFinished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    // the long used for count
    return 8L;
  }

  @Override
  public long calculateMaxReturnSize() {
    // the long used for count
    return 8L;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public void close() throws Exception {
    if (schemaReader != null) {
      schemaReader.close();
      schemaReader = null;
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId);
  }
}
