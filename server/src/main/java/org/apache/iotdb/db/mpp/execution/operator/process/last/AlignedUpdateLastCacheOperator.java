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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import static org.weakref.jmx.internal.guava.base.Preconditions.checkArgument;

/** update last cache for aligned series */
public class AlignedUpdateLastCacheOperator extends AbstractUpdateLastCacheOperator {

  private final AlignedPath seriesPath;

  private final PartialPath devicePath;

  public AlignedUpdateLastCacheOperator(
      OperatorContext operatorContext,
      Operator child,
      AlignedPath seriesPath,
      DataNodeSchemaCache dataNodeSchemaCache,
      boolean needUpdateCache) {
    super(operatorContext, child, dataNodeSchemaCache, needUpdateCache);
    this.seriesPath = seriesPath;
    this.devicePath = seriesPath.getDevicePath();
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

    tsBlockBuilder.reset();
    for (int i = 0; i + 1 < res.getValueColumnCount(); i += 2) {
      if (!res.getColumn(i).isNull(0)) {
        long lastTime = res.getColumn(i).getLong(0);
        TsPrimitiveType lastValue = res.getColumn(i + 1).getTsPrimitiveType(0);
        MeasurementPath measurementPath =
            new MeasurementPath(
                devicePath.concatNode(seriesPath.getMeasurementList().get(i / 2)),
                seriesPath.getSchemaList().get(i / 2),
                true);
        if (needUpdateCache) {
          TimeValuePair timeValuePair = new TimeValuePair(lastTime, lastValue);
          lastCache.updateLastCache(
              getDatabaseName(), measurementPath, timeValuePair, false, Long.MIN_VALUE);
        }
        LastQueryUtil.appendLastValue(
            tsBlockBuilder,
            lastTime,
            measurementPath.getFullPath(),
            lastValue.getStringValue(),
            seriesPath.getSchemaList().get(i / 2).getType().name());
      }
    }
    return !tsBlockBuilder.isEmpty() ? tsBlockBuilder.build() : LAST_QUERY_EMPTY_TSBLOCK;
  }
}
