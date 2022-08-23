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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.Set;

public class SeriesScanOperator implements DataSourceOperator {

  private final OperatorContext operatorContext;
  private final SeriesScanUtil seriesScanUtil;
  private final PlanNodeId sourceId;
  private TsBlock tsBlock;
  private boolean hasCachedTsBlock = false;
  private boolean finished = false;

  private final long maxReturnSize;

  public SeriesScanOperator(
      PlanNodeId sourceId,
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      OperatorContext context,
      Filter timeFilter,
      Filter valueFilter,
      boolean ascending) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil =
        new SeriesScanUtil(
            seriesPath,
            allSensors,
            dataType,
            context.getInstanceContext(),
            timeFilter,
            valueFilter,
            ascending);
    this.maxReturnSize = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    if (hasCachedTsBlock || hasNext()) {
      hasCachedTsBlock = false;
      TsBlock res = tsBlock;
      tsBlock = null;
      return res;
    }
    throw new IllegalStateException("no next batch");
  }

  @Override
  public boolean hasNext() {

    try {
      if (hasCachedTsBlock) {
        return true;
      }

      /*
       * consume page data firstly
       */
      if (readPageData()) {
        hasCachedTsBlock = true;
        return true;
      }

      /*
       * consume chunk data secondly
       */
      if (readChunkData()) {
        hasCachedTsBlock = true;
        return true;
      }

      /*
       * consume next file finally
       */
      while (seriesScanUtil.hasNextFile()) {
        if (readChunkData()) {
          hasCachedTsBlock = true;
          return true;
        }
      }
      return hasCachedTsBlock;
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }
  }

  @Override
  public boolean isFinished() {
    return finished || (finished = !hasNext());
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

  private boolean readChunkData() throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readPageData() throws IOException {
    while (seriesScanUtil.hasNextPage()) {
      tsBlock = seriesScanUtil.nextPage();
      if (!isEmpty(tsBlock)) {
        return true;
      }
    }
    return false;
  }

  private boolean isEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.isEmpty();
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
