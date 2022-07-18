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
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.Map;

public class LevelTimeSeriesCountOperator implements SourceOperator {
  private final PlanNodeId sourceId;
  private final OperatorContext operatorContext;
  private final PartialPath partialPath;
  private final boolean isPrefixPath;
  private final int level;

  private boolean isFinished;

  public LevelTimeSeriesCountOperator(
      PlanNodeId sourceId,
      OperatorContext operatorContext,
      PartialPath partialPath,
      boolean isPrefixPath,
      int level) {
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.partialPath = partialPath;
    this.isPrefixPath = isPrefixPath;
    this.level = level;
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
    isFinished = true;
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(HeaderConstant.countLevelTimeSeriesHeader.getRespDataTypes());
    Map<PartialPath, Integer> countMap;
    try {
      countMap =
          ((SchemaDriverContext) operatorContext.getInstanceContext().getDriverContext())
              .getSchemaRegion()
              .getMeasurementCountGroupByLevel(partialPath, level, isPrefixPath);
    } catch (MetadataException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    countMap.forEach(
        (path, count) -> {
          tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
          tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(path.getFullPath()));
          tsBlockBuilder.getColumnBuilder(1).writeInt(count);
          tsBlockBuilder.declarePosition();
        });
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    return !isFinished;
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }
}
