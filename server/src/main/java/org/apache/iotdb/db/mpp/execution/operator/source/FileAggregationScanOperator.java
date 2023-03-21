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

package org.apache.iotdb.db.mpp.execution.operator.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.io.IOException;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class FileAggregationScanOperator extends AbstractSourceOperator
    implements DataSourceOperator, SourceOperator {

  private final FileAggregationScanUtil aggregationScanUtil;

  private final TsBlockBuilder tsBlockBuilder;

  public FileAggregationScanOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      PartialPath pathPattern,
      AggregationDescriptor aggregationDescriptor,
      int[] levels) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.aggregationScanUtil =
        new FileAggregationScanUtil(
            pathPattern, aggregationDescriptor, levels, new SeriesScanOptions.Builder().build());
    this.tsBlockBuilder = LastQueryUtil.createTsBlockBuilder();
  }

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {
    aggregationScanUtil.initQueryDataSource(dataSource);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public boolean hasNext() throws Exception {
    return aggregationScanUtil.hasNextFile();
  }

  @Override
  public TsBlock next() throws Exception {
    tsBlockBuilder.reset();
    try {
      aggregationScanUtil.consume();
      return aggregationScanUtil.getAggregationResult(tsBlockBuilder);
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }
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
    return 0;
  }
}
