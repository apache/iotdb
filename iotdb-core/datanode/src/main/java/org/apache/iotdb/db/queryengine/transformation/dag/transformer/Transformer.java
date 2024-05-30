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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer;

import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;

import org.apache.tsfile.block.column.Column;

public abstract class Transformer implements LayerReader {

  protected Column[] cachedColumns;
  protected int cacheConsumed;

  protected Transformer() {}

  @Override
  public final YieldableState yield() throws Exception {
    if (cachedColumns != null && cacheConsumed < cachedColumns[0].getPositionCount()) {
      return YieldableState.YIELDABLE;
    }

    // Put concrete logic into yieldValue function call
    return yieldValue();
  }

  @Override
  public void consumedAll() {
    invalidCache();
  }

  @Override
  public Column[] current() {
    if (cacheConsumed == 0) {
      return cachedColumns;
    }

    Column[] ret = new Column[cachedColumns.length];
    for (int i = 0; i < cachedColumns.length; i++) {
      ret[i] = cachedColumns[i].subColumn(cacheConsumed);
    }

    return ret;
  }

  private void invalidCache() {
    cacheConsumed = 0;
    cachedColumns = null;
  }

  protected abstract YieldableState yieldValue() throws Exception;
}
