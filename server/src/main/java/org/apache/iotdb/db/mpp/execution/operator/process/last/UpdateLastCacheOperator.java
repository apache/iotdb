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
package org.apache.iotdb.db.mpp.execution.operator.process.last;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;

public class UpdateLastCacheOperator extends AbstractUpdateLastCacheOperator {

  // fullPath for queried time series
  // It should be exact PartialPath, neither MeasurementPath nor AlignedPath, because lastCache only
  // accept PartialPath
  private MeasurementPath fullPath;

  // dataType for queried time series;
  private String dataType;

  public UpdateLastCacheOperator(
      OperatorContext operatorContext,
      Operator child,
      MeasurementPath fullPath,
      TSDataType dataType,
      DataNodeSchemaCache dataNodeSchemaCache,
      boolean needUpdateCache) {
    super(operatorContext, child, dataNodeSchemaCache, needUpdateCache);
    this.fullPath = fullPath;
    this.dataType = dataType.name();
  }

  @Override
  public TsBlock next() {
    TsBlock res = child.nextWithTimer();
    if (res == null) {
      return null;
    }
    if (res.isEmpty()) {
      return LAST_QUERY_EMPTY_TSBLOCK;
    }

    checkArgument(res.getPositionCount() == 1, "last query result should only have one record");

    // last value is null
    if (res.getColumn(0).isNull(0)) {
      return LAST_QUERY_EMPTY_TSBLOCK;
    }

    long lastTime = res.getColumn(0).getLong(0);
    TsPrimitiveType lastValue = res.getColumn(1).getTsPrimitiveType(0);

    if (needUpdateCache) {
      TimeValuePair timeValuePair = new TimeValuePair(lastTime, lastValue);
      lastCache.updateLastCache(getDatabaseName(), fullPath, timeValuePair, false, Long.MIN_VALUE);
    }

    tsBlockBuilder.reset();

    LastQueryUtil.appendLastValue(
        tsBlockBuilder, lastTime, fullPath.getFullPath(), lastValue.getStringValue(), dataType);

    return tsBlockBuilder.build();
  }
}
