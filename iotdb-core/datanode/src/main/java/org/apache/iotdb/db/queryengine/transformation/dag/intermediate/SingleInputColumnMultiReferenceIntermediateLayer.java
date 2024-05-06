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

package org.apache.iotdb.db.queryengine.transformation.dag.intermediate;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.dag.memory.SafetyLine;
import org.apache.iotdb.db.queryengine.transformation.dag.memory.SafetyLine.SafetyPile;
import org.apache.iotdb.db.queryengine.transformation.dag.util.LayerCacheUtils;
import org.apache.iotdb.db.queryengine.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.iterator.TVListForwardIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleInputColumnMultiReferenceIntermediateLayer extends IntermediateLayer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SingleInputColumnMultiReferenceIntermediateLayer.class);

  private final LayerReader parentLayerReader;
  private final TSDataType parentLayerReaderDataType;
  private final boolean isParentLayerReaderConstant;
  private final ElasticSerializableTVList tvList;
  private final SafetyLine safetyLine;

  public SingleInputColumnMultiReferenceIntermediateLayer(
      Expression expression,
      String queryId,
      float memoryBudgetInMB,
      LayerReader parentLayerReader) {
    super(expression, queryId, memoryBudgetInMB);
    this.parentLayerReader = parentLayerReader;

    parentLayerReaderDataType = this.parentLayerReader.getDataTypes()[0];
    isParentLayerReaderConstant = this.parentLayerReader.isConstantPointReader();
    tvList =
        ElasticSerializableTVList.newElasticSerializableTVList(
            parentLayerReaderDataType, queryId, memoryBudgetInMB, CACHE_BLOCK_SIZE);
    safetyLine = new SafetyLine();
  }

  @Override
  public LayerReader constructReader() {
    return new LayerReader() {
      private final SafetyPile safetyPile = safetyLine.addSafetyPile();

      private TimeColumn cachedTimes = null;
      private Column cachedValues = null;
      private int cacheConsumed = 0;
      private TVListForwardIterator iterator = tvList.constructIterator();

      @Override
      public boolean isConstantPointReader() {
        return isParentLayerReaderConstant;
      }

      @Override
      public YieldableState yield() throws Exception {
        // Column cached in reader is not yet consumed
        if (cachedTimes != null && cacheConsumed < cachedTimes.getPositionCount()) {
          return YieldableState.YIELDABLE;
        }

        // TVList still has some cached columns
        if (iterator.hasNext()) {
          iterator.next();
          cachedTimes = iterator.currentTimes();
          cachedValues = iterator.currentValues();

          return YieldableState.YIELDABLE;
        }

        // No data cached, yield from parent layer reader
        YieldableState state = LayerCacheUtils.yieldPoints(parentLayerReader, tvList);
        if (state == YieldableState.YIELDABLE) {
          iterator.next();
          cachedTimes = iterator.currentTimes();
          cachedValues = iterator.currentValues();
        }
        return state;
      }

      @Override
      public void consumed(int consumed) {
        assert cacheConsumed + consumed <= cachedTimes.getPositionCount();
        cacheConsumed += consumed;

        safetyPile.moveForward(consumed);
        tvList.setEvictionUpperBound(safetyLine.getSafetyLine());

        // Invalid cache
        if (cacheConsumed == cachedTimes.getPositionCount()) {
          cacheConsumed = 0;
          cachedTimes = null;
          cachedValues = null;
        }
      }

      @Override
      public void consumedAll() {
        int steps = cachedTimes.getPositionCount() - cacheConsumed;
        safetyPile.moveForward(steps);
        tvList.setEvictionUpperBound(safetyLine.getSafetyLine());

        cacheConsumed = 0;
        cachedTimes = null;
        cachedValues = null;
      }

      @Override
      public Column[] current() {
        return cacheConsumed == 0
            ? new Column[] {cachedValues, cachedTimes}
            : new Column[] {
              cachedValues.subColumn(cacheConsumed), cachedTimes.subColumn(cacheConsumed)
            };
      }

      @Override
      public TSDataType[] getDataTypes() {
        return new TSDataType[] {parentLayerReaderDataType};
      }
    };
  }
}
