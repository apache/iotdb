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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;

/** Rewrites files through the regular tablet write pipeline without staging plain chunks. */
public class TabletRewriteLoadStrategy implements TsFileLoadStrategy {

  private final LoadTsFileDataTypeConverter converter;
  private final boolean isGeneratedByPipe;
  private final LoadTsFileCostMetricsSet costMetrics;

  public TabletRewriteLoadStrategy(
      final MPPQueryContext queryContext, final boolean isGeneratedByPipe) {
    this(
        new LoadTsFileDataTypeConverter(queryContext, isGeneratedByPipe),
        isGeneratedByPipe,
        LoadTsFileCostMetricsSet.getInstance());
  }

  TabletRewriteLoadStrategy(
      final LoadTsFileDataTypeConverter converter,
      final boolean isGeneratedByPipe,
      final LoadTsFileCostMetricsSet costMetrics) {
    this.converter = converter;
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.costMetrics = costMetrics;
  }

  @Override
  public boolean execute(final LoadSingleTsFileNode node) throws LoadFileException {
    final long startTime = System.nanoTime();
    try {
      // Only the scheduler may delete the source, after the entire strategy has succeeded.
      return converter.isSuccessful(
          LoadFallbackHandler.convertTsFileToTablets(converter, node, isGeneratedByPipe, false));
    } catch (final Exception e) {
      throw new LoadFileException(e);
    } finally {
      costMetrics.recordPhaseTimeCost(
          LoadTsFileCostMetricsSet.SCHEDULER_CAST_TABLETS, System.nanoTime() - startTime);
    }
  }
}
