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
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.ISchemaSource;
import org.apache.iotdb.db.queryengine.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ISchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class SchemaQueryScanOperator<T extends ISchemaInfo> implements SourceOperator {
  private static final long DEFAULT_MAX_TS_BLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  private static final long MAX_SIZE = DEFAULT_MAX_TS_BLOCK_SIZE_IN_BYTES;

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SchemaQueryScanOperator.class);

  protected PlanNodeId sourceId;

  protected OperatorContext operatorContext;

  private final ISchemaSource<T> schemaSource;

  private long limit = -1;
  private long offset = 0;

  private String database;

  private final List<TSDataType> outputDataTypes;

  private ISchemaReader<T> schemaReader;

  private final TsBlockBuilder tsBlockBuilder;
  private ListenableFuture<?> isBlocked;
  private TsBlock next;
  private boolean isFinished;
  private long count = 0;

  public SchemaQueryScanOperator(
      final PlanNodeId sourceId,
      final OperatorContext operatorContext,
      final ISchemaSource<T> schemaSource) {
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.schemaSource = schemaSource;
    this.outputDataTypes =
        schemaSource.getInfoQueryColumnHeaders().stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
  }

  protected ISchemaReader<T> createSchemaReader() {
    return schemaSource.getSchemaReader(
        ((SchemaDriverContext) operatorContext.getDriverContext()).getSchemaRegion());
  }

  private void setColumns(final T element, final TsBlockBuilder builder) {
    schemaSource.transformToTsBlockColumns(element, builder, getDatabase());
  }

  public void setLimit(final long limit) {
    this.limit = limit;
  }

  public void setOffset(final long offset) {
    this.offset = offset;
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
   * Try to get next {@link TsBlock}. If the next is not ready, return a future. After success,
   * {@link SchemaQueryScanOperator#next} will be set.
   */
  private ListenableFuture<?> tryGetNext() {
    if (schemaReader == null) {
      schemaReader = createSchemaReader();
    }
    while (true) {
      try {
        final ListenableFuture<?> readerBlocked = schemaReader.isBlocked();
        if (!readerBlocked.isDone()) {
          final SettableFuture<?> settableFuture = SettableFuture.create();
          readerBlocked.addListener(
              () -> {
                next = tsBlockBuilder.build();
                tsBlockBuilder.reset();
                settableFuture.set(null);
              },
              directExecutor());
          return settableFuture;
        } else if (schemaReader.hasNext() && (limit < 0 || count < offset + limit)) {
          final T element = schemaReader.next();
          if (++count > offset) {
            setColumns(element, tsBlockBuilder);
            if (tsBlockBuilder.getRetainedSizeInBytes() >= MAX_SIZE) {
              next = tsBlockBuilder.build();
              tsBlockBuilder.reset();
              return NOT_BLOCKED;
            }
          }
        } else {
          if (tsBlockBuilder.isEmpty()) {
            next = null;
            isFinished = true;
          } else {
            next = tsBlockBuilder.build();
          }
          tsBlockBuilder.reset();
          return NOT_BLOCKED;
        }
      } catch (final Exception e) {
        throw new SchemaExecutionException(e);
      }
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final TsBlock ret = next;
    next = null;
    isBlocked = null;
    return ret;
  }

  @Override
  public boolean hasNext() throws Exception {
    isBlocked().get(); // wait for the next TsBlock
    if (!schemaReader.isSuccess()) {
      throw new SchemaExecutionException(schemaReader.getFailure());
    }
    return next != null;
  }

  @Override
  public boolean isFinished() throws Exception {
    return isFinished;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return DEFAULT_MAX_TS_BLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateMaxReturnSize() {
    return DEFAULT_MAX_TS_BLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  protected String getDatabase() {
    if (database == null) {
      database =
          ((SchemaDriverContext) operatorContext.getDriverContext())
              .getSchemaRegion()
              .getDatabaseFullPath();
    }
    return database;
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
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
        + RamUsageEstimator.sizeOf(database);
  }
}
