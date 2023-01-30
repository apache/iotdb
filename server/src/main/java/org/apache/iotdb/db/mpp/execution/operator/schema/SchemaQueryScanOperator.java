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
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.schema.source.ISchemaSource;
import org.apache.iotdb.db.mpp.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.util.List;
import java.util.NoSuchElementException;
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
  }

  protected ISchemaReader<T> createSchemaReader() {
    return schemaSource.getSchemaReader(
        ((SchemaDriverContext) operatorContext.getDriverContext()).getSchemaRegion());
  }

  protected void setColumns(T element, TsBlockBuilder builder) {
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
  public TsBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    T element;
    while (schemaReader.hasNext()) {
      element = schemaReader.next();
      setColumns(element, tsBlockBuilder);
      if (tsBlockBuilder.getRetainedSizeInBytes() >= MAX_SIZE) {
        break;
      }
    }
    if (!schemaReader.isSuccess()) {
      throw new RuntimeException(schemaReader.getFailure());
    }
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    if (schemaReader == null) {
      schemaReader = createSchemaReader();
    }
    return schemaReader.hasNext();
  }

  @Override
  public boolean isFinished() {
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
              .getStorageGroupFullPath();
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
