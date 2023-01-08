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
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowNodesPlan;
import org.apache.iotdb.db.metadata.query.info.INodeSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
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

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class NodePathsSchemaScanOperator implements SourceOperator {

  private static final long MAX_SIZE = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

  private final PlanNodeId sourceId;

  private final OperatorContext operatorContext;

  private final PartialPath partialPath;

  private final int level;

  private boolean isFinished;

  private final List<TSDataType> outputDataTypes;

  private ISchemaReader<INodeSchemaInfo> nodeReader;

  public NodePathsSchemaScanOperator(
      PlanNodeId sourceId, OperatorContext operatorContext, PartialPath partialPath, int level) {
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.partialPath = partialPath;
    this.level = level;
    this.outputDataTypes =
        ColumnHeaderConstant.showChildPathsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    isFinished = true;
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    INodeSchemaInfo nodeSchemaInfo;
    while (nodeReader.hasNext()) {
      nodeSchemaInfo = nodeReader.next();
      tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
      tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(nodeSchemaInfo.getFullPath()));
      tsBlockBuilder
          .getColumnBuilder(1)
          .writeBinary(new Binary(String.valueOf(nodeSchemaInfo.getNodeType().getNodeType())));
      tsBlockBuilder.declarePosition();
      if (tsBlockBuilder.getRetainedSizeInBytes() >= MAX_SIZE) {
        break;
      }
    }
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    if (nodeReader == null) {
      nodeReader = createReader();
    }
    return nodeReader.hasNext();
  }

  private ISchemaReader<INodeSchemaInfo> createReader() {
    IShowNodesPlan showNodesPlan;
    if (-1 == level) {
      showNodesPlan =
          SchemaRegionReadPlanFactory.getShowNodesPlan(
              partialPath.concatNode(ONE_LEVEL_PATH_WILDCARD));
    } else {
      showNodesPlan = SchemaRegionReadPlanFactory.getShowNodesPlan(partialPath, level, false);
    }
    try {
      return nodeReader =
          ((SchemaDriverContext) operatorContext.getDriverContext())
              .getSchemaRegion()
              .getNodeReader(showNodesPlan);
    } catch (MetadataException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
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

  @Override
  public void close() throws Exception {
    if (nodeReader != null) {
      nodeReader.close();
      nodeReader = null;
    }
  }
}
