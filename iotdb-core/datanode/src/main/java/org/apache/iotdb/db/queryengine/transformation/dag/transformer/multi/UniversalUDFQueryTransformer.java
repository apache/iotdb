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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.multi;

import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.db.queryengine.transformation.datastructure.iterator.TVListForwardIterator;
import org.apache.iotdb.db.queryengine.transformation.datastructure.tv.ElasticSerializableTVList;

import org.apache.tsfile.block.column.Column;

import java.io.IOException;

public abstract class UniversalUDFQueryTransformer extends UDFQueryTransformer {
  protected final ElasticSerializableTVList outputStorage;
  protected final TVListForwardIterator outputLayerIterator;

  protected UniversalUDFQueryTransformer(UDTFExecutor executor) {
    super(executor);
    outputStorage = executor.getOutputStorage();
    outputLayerIterator = outputStorage.constructIterator();
  }

  @Override
  protected final YieldableState yieldValue() throws Exception {
    while (!cacheValueFromUDFOutput()) {
      final YieldableState udfYieldableState = tryExecuteUDFOnce();
      if (udfYieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
        return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
      }
      if (udfYieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA && !terminate()) {
        return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
      }
    }
    return YieldableState.YIELDABLE;
  }

  protected abstract YieldableState tryExecuteUDFOnce() throws Exception;

  protected final boolean cacheValueFromUDFOutput() throws IOException {
    if (!outputLayerIterator.hasNext()) {
      return false;
    }
    outputLayerIterator.next();

    Column values = outputLayerIterator.currentValues();
    Column times = outputLayerIterator.currentTimes();
    cachedColumns = new Column[] {values, times};

    outputStorage.setEvictionUpperBound(outputLayerIterator.getEndPointIndex());

    return true;
  }

  @Override
  public final boolean isConstantPointReader() {
    return false;
  }
}
