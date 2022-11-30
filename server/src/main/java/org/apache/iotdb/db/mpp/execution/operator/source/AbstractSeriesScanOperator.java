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

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public abstract class AbstractSeriesScanOperator implements DataSourceOperator {

  private final OperatorContext operatorContext;
  private final SeriesScanUtil seriesScanUtil;
  private final PlanNodeId sourceId;

  private boolean finished = false;

  private final TsBlockBuilder resultBuilder;

  private final long maxReturnSize;

  public AbstractSeriesScanOperator(
      PlanNodeId sourceId,
      SeriesScanUtil seriesScanUtil,
      OperatorContext context,
      long maxReturnSize) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil = seriesScanUtil;
    this.resultBuilder = seriesScanUtil.getCachedTsBlockBuilder();
    this.maxReturnSize = maxReturnSize;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    TsBlock block = resultBuilder.build();
    resultBuilder.reset();
    return block;
  }

  @Override
  public boolean hasNext() {
    try {
      // start stopwatch
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();

      // here use do-while to promise doing this at least once
      do {
        // consume page data firstly
        if (readPageData()) {
          continue;
        }

        // consume chunk data secondly
        if (readChunkData()) {
          continue;
        }

        // consume next file finally
        if (readFileData()) {
          continue;
        }
        break;

      } while (System.nanoTime() - start < maxRuntime && !resultBuilder.isFull());

      finished = resultBuilder.isEmpty();
      return !finished;
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return maxReturnSize;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  private boolean readFileData() throws IOException {
    while (seriesScanUtil.hasNextFile()) {
      if (readChunkData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readChunkData() throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readPageData() throws IOException {
    return seriesScanUtil.tryToFetchDataFromPage(true);
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {
    seriesScanUtil.initQueryDataSource(dataSource);
  }
}
