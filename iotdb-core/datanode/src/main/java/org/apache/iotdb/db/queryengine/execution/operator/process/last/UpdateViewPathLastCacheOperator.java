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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

public class UpdateViewPathLastCacheOperator extends UpdateLastCacheOperator {

  private final String outputViewPath;

  public UpdateViewPathLastCacheOperator(
      OperatorContext operatorContext,
      Operator child,
      MeasurementPath fullPath,
      TSDataType dataType,
      TreeDeviceSchemaCacheManager treeDeviceSchemaCacheManager,
      boolean needUpdateCache,
      boolean needUpdateNullEntry,
      String outputViewPath) {
    super(
        operatorContext,
        child,
        fullPath,
        dataType,
        treeDeviceSchemaCacheManager,
        needUpdateCache,
        needUpdateNullEntry);
    this.outputViewPath = outputViewPath;
  }

  @Override
  protected void appendLastValueToTsBlockBuilder(long lastTime, TsPrimitiveType lastValue) {
    LastQueryUtil.appendLastValue(
        tsBlockBuilder, lastTime, outputViewPath, lastValue.getStringValue(), dataType);
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed() + RamUsageEstimator.sizeOf(outputViewPath);
  }
}
