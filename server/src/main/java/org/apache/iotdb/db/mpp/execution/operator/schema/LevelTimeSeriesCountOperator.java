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
import org.apache.iotdb.db.metadata.schemainfo.LevelTimeSeriesCountSchemaInfo;
import org.apache.iotdb.db.metadata.schemareader.ISchemaReader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class LevelTimeSeriesCountOperator implements SourceOperator {
  private final PlanNodeId sourceId;
  private final OperatorContext operatorContext;
  private final PartialPath partialPath;
  private final boolean isPrefixPath;
  private final int level;
  private final String key;
  private final String value;
  private final boolean isContains;

  private List<TsBlock> tsBlockList;
  private int currentIndex = 0;

  private ISchemaReader<? extends ISchemaInfo> schemaReader;

  private final List<TSDataType> outputDataTypes;

  public LevelTimeSeriesCountOperator(
      PlanNodeId sourceId,
      OperatorContext operatorContext,
      PartialPath partialPath,
      boolean isPrefixPath,
      int level,
      String key,
      String value,
      boolean isContains) {
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.partialPath = partialPath;
    this.isPrefixPath = isPrefixPath;
    this.level = level;
    this.key = key;
    this.value = value;
    this.isContains = isContains;
    this.outputDataTypes =
        ColumnHeaderConstant.countLevelTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
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
  public TsBlock next() {
    return SchemaTsBlockUtil.transferSchemaResultToTsBlock(
        schemaReader.next(), outputDataTypes, this::setColumns);
  }

  @Override
  public boolean hasNext() {
    if (schemaReader == null) {
      try {

        schemaReader =
            ((SchemaDriverContext) operatorContext.getInstanceContext().getDriverContext())
                .getSchemaRegion()
                .getLevelTimeSeriesCountSchemaInfoReader(
                    partialPath, level, isPrefixPath, key, value, isContains);
      } catch (MetadataException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
    return schemaReader.hasNext();
  }

  protected void setColumns(ISchemaInfo iSchemaInfo, TsBlockBuilder tsBlockBuilder) {
    LevelTimeSeriesCountSchemaInfo.CountEntry countEntry =
        ((LevelTimeSeriesCountSchemaInfo) iSchemaInfo).getCountEntry();
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(countEntry.path.getFullPath()));
    tsBlockBuilder.getColumnBuilder(1).writeInt(countEntry.count);
    tsBlockBuilder.declarePosition();
  }

  @Override
  public boolean isFinished() {
    return !hasNext();
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
