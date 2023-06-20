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

import com.google.common.util.concurrent.Futures;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.db.metadata.query.info.ISchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.schema.source.ISchemaSource;
import org.apache.iotdb.db.mpp.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class SchemaQueryScanOperator<T extends ISchemaInfo> implements SourceOperator {

  private static final long MAX_SIZE = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

  protected PlanNodeId sourceId;

  protected OperatorContext operatorContext;

  private final ISchemaSource<T> schemaSource;

  protected int limit;
  protected int offset;
  protected PartialPath partialPath;
  protected boolean isPrefixPath;

  private String database;

  private final List<TSDataType> outputDataTypes;

  private ISchemaReader<T> schemaReader;

  private final TsBlockBuilder tsBlockBuilder;

  private ListenableFuture<Boolean> blockedHasNext;


  private ListenableFuture<Void> isBlocked;
  private TsBlock next;

  protected SchemaQueryScanOperator(
      PlanNodeId sourceId,
      OperatorContext operatorContext,
      int limit,
      int offset,
      PartialPath partialPath,
      boolean isPrefixPath,
      List<TSDataType> outputDataTypes) {
    this.operatorContext = operatorContext;
    this.limit = limit;
    this.offset = offset;
    this.partialPath = partialPath;
    this.isPrefixPath = isPrefixPath;
    this.sourceId = sourceId;
    this.outputDataTypes = outputDataTypes;
    this.schemaSource = null;
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
  }

  public SchemaQueryScanOperator(
      PlanNodeId sourceId, OperatorContext operatorContext, ISchemaSource<T> schemaSource) {
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

  private void setColumns(T element, TsBlockBuilder builder) {
    schemaSource.transformToTsBlockColumns(element, builder, getDatabase());
  }

  public PartialPath getPartialPath() {
    return partialPath;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public boolean isPrefixPath() {
    return isPrefixPath;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if(isBlocked==null){
      isBlocked = tryGetNext();
    }
    return isBlocked;
  }

  /**
   * Try to get next TsBlock. If the next is not ready, return a future. After success, {@link SchemaQueryScanOperator#next} will be set.
   */
  private ListenableFuture<Void> tryGetNext(){
    if (schemaReader == null) {
      schemaReader = createSchemaReader();
    }
    return Futures.submit(
        () -> {

          ListenableFuture<Boolean> hasNextFuture = schemaReader.hasNextFuture();

          if (schemaReader.hasNext()) {
            T element = schemaReader.next();
            setColumns(element, tsBlockBuilder);
            return null;
          } else {
            return null;
          }
        }, null);
    if (!tsBlockBuilder.isEmpty()) {
      return true;
    } else {
      ListenableFuture<Boolean> hasNextFuture = schemaReader.hasNextFuture();
      Futures.addCallback();
      if (!hasNextFuture.isDone()) {
        blockedHasNext = hasNextFuture;
        // we do not know whether it has next or not now, so return true
        return true;
      } else {
        // hasNextFuture is done but may not success
        if (!schemaReader.isSuccess()) {
          throw new RuntimeException(schemaReader.getFailure());
        }
        return hasNextFuture.get();
      }
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (blockedHasNext != null && !blockedHasNext.isDone()) {
      // if blocked, return null
      return null;
    }
    boolean hasNext = blockedHasNext == null || blockedHasNext.get();
    while (hasNext) {
      T element = schemaReader.next();
      if (element != null) {
        setColumns(element, tsBlockBuilder);
        if (tsBlockBuilder.getRetainedSizeInBytes() >= MAX_SIZE) {
          break;
        }
      }
      ListenableFuture<Boolean> future = schemaReader.hasNextFuture();
      if (!future.isDone()) {
        blockedHasNext = future;
        return null;
      }
      hasNext = future.get();
    }

    if (!schemaReader.isSuccess()) {
      throw new RuntimeException(schemaReader.getFailure());
    }
    TsBlock res = tsBlockBuilder.build();
    tsBlockBuilder.reset();
    blockedHasNext = null;
    return res;
  }

  @Override
  public boolean hasNext() throws Exception {
    if (schemaReader == null) {
      schemaReader = createSchemaReader();
    }
    if (!tsBlockBuilder.isEmpty()) {
      return true;
    } else {
      ListenableFuture<Boolean> hasNextFuture = schemaReader.hasNextFuture();
      if (!hasNextFuture.isDone()) {
        blockedHasNext = hasNextFuture;
        // we do not know whether it has next or not now, so return true
        return true;
      } else {
        // hasNextFuture is done but may not success
        if (!schemaReader.isSuccess()) {
          throw new RuntimeException(schemaReader.getFailure());
        }
        return hasNextFuture.get();
      }
    }
  }

  //  private ListenableFuture<TsBlock> tryGetNext(){
  //
  //  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNextWithTimer();
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
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
}
