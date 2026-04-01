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

package org.apache.iotdb.db.queryengine.execution.operator.process.last;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class AlignedUpdateViewPathLastCacheOperator extends AlignedUpdateLastCacheOperator {
  // Now not only a view path will be set here, but also the measurement path with alias will be set
  // .e.g last query path: root.test.d1(s1(alias1), s2), outputPaths: [root.test.d1.alias1, null]
  private final List<String> outputPaths;
  private int outputPathIndex = 0;

  public AlignedUpdateViewPathLastCacheOperator(
      OperatorContext operatorContext,
      Operator child,
      AlignedPath seriesPath,
      TreeDeviceSchemaCacheManager treeDeviceSchemaCacheManager,
      boolean needUpdateCache,
      boolean needUpdateNullEntry,
      List<String> outputPaths,
      boolean deviceInMultiRegion) {
    super(
        operatorContext,
        child,
        seriesPath,
        treeDeviceSchemaCacheManager,
        needUpdateCache,
        needUpdateNullEntry,
        deviceInMultiRegion);
    checkArgument(outputPaths != null, "outputPaths shouldn't be null");
    this.outputPaths = outputPaths;
  }

  @Override
  protected void appendLastValueToTsBlockBuilder(
      long lastTime, TsPrimitiveType lastValue, MeasurementPath measurementPath, String type) {
    String outputPath = outputPaths.get(outputPathIndex);
    LastQueryUtil.appendLastValueRespectBlob(
        tsBlockBuilder,
        lastTime,
        outputPath == null ? measurementPath.getFullPath() : outputPath,
        lastValue,
        type);
    outputPathIndex++;
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed()
        + outputPaths.stream().mapToLong(path -> RamUsageEstimator.sizeOf(path)).sum();
  }
}
