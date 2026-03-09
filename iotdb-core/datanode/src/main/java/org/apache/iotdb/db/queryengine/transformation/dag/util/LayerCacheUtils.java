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

package org.apache.iotdb.db.queryengine.transformation.dag.util;

import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.dag.input.IUDFInputDataSet;
import org.apache.iotdb.db.queryengine.transformation.datastructure.row.ElasticSerializableRowList;
import org.apache.iotdb.db.queryengine.transformation.datastructure.tv.ElasticSerializableTVList;

import org.apache.tsfile.block.column.Column;

public class LayerCacheUtils {

  private LayerCacheUtils() {}

  public static YieldableState yieldPoints(LayerReader source, ElasticSerializableTVList target)
      throws Exception {
    final YieldableState yieldableState = source.yield();
    if (yieldableState != YieldableState.YIELDABLE) {
      return yieldableState;
    }

    // Source would generate two columns:
    // First column is the value column;
    // Second column is always the time column.
    Column[] columns = source.current();
    target.putColumn(columns[1], columns[0]);
    source.consumedAll();

    return YieldableState.YIELDABLE;
  }

  public static YieldableState yieldPoints(
      LayerReader source, ElasticSerializableTVList target, int count) throws Exception {
    while (count > 0) {
      final YieldableState yieldableState = source.yield();
      if (yieldableState != YieldableState.YIELDABLE) {
        return yieldableState;
      }

      Column[] columns = source.current();
      target.putColumn(columns[1], columns[0]);
      source.consumedAll();

      int size = columns[0].getPositionCount();
      count -= size;
    }

    return YieldableState.YIELDABLE;
  }

  public static YieldableState yieldRows(
      IUDFInputDataSet source, ElasticSerializableRowList target, int count) throws Exception {
    while (count > 0) {
      final YieldableState yieldableState = source.yield();
      if (yieldableState != YieldableState.YIELDABLE) {
        return yieldableState;
      }

      Column[] columns = source.currentBlock();
      target.put(columns);

      int size = columns[0].getPositionCount();
      count -= size;
    }

    return YieldableState.YIELDABLE;
  }
}
