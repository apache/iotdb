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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.schemainfo.ISchemaInfo;
import org.apache.iotdb.db.metadata.schemareader.ISchemaReader;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.util.List;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public abstract class SchemaQueryScanOperator implements SourceOperator {

  protected OperatorContext operatorContext;
  protected List<TsBlock> tsBlockList;
  protected int currentIndex = 0;

  protected List<TSDataType> outputDataTypes;

  protected int limit;
  protected int offset;
  protected PartialPath partialPath;
  protected boolean isPrefixPath;

  protected PlanNodeId sourceId;

  protected ISchemaReader schemaReader;

  protected SchemaQueryScanOperator(
      PlanNodeId sourceId,
      OperatorContext operatorContext,
      int limit,
      int offset,
      PartialPath partialPath,
      boolean isPrefixPath) {
    this.operatorContext = operatorContext;
    this.limit = limit;
    this.offset = offset;
    this.partialPath = partialPath;
    this.isPrefixPath = isPrefixPath;
    this.sourceId = sourceId;
  }

  protected abstract void setColumns(ISchemaInfo iSchemaInfo, TsBlockBuilder builder);

  protected abstract ISchemaReader<? extends ISchemaInfo> createSchemaReader()
      throws MetadataException;

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
    return SchemaTsBlockUtil.transferSchemaResultToTsBlock(
        schemaReader.next(), outputDataTypes, this::setColumns);
  }

  @Override
  public boolean hasNext() {
    if (schemaReader == null) {
      try {
        schemaReader = createSchemaReader();
      } catch (MetadataException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
    return schemaReader.hasNext();
  }

  @Override
  public boolean isFinished() {
    return !hasNext();
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
}
