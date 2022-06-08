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

package org.apache.iotdb.db.mpp.transformation.dag.transformer;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;

public abstract class Transformer implements LayerPointReader {

  protected boolean hasCachedValue;

  protected long cachedTime;

  protected int cachedInt;
  protected long cachedLong;
  protected float cachedFloat;
  protected double cachedDouble;
  protected boolean cachedBoolean;
  protected Binary cachedBinary;
  protected boolean currentNull;

  protected Transformer() {
    hasCachedValue = false;
  }

  @Override
  public final boolean next() throws QueryProcessException, IOException {
    if (!hasCachedValue) {
      hasCachedValue = cacheValue();
    }
    return hasCachedValue;
  }

  /** if this method returns true, at least one of the cached field should be set */
  protected abstract boolean cacheValue() throws QueryProcessException, IOException;

  @Override
  public final YieldableState yield() throws IOException, QueryProcessException {
    if (hasCachedValue) {
      return YieldableState.YIELDABLE;
    }

    final YieldableState yieldableState = yieldValue();
    if (YieldableState.YIELDABLE == yieldableState) {
      hasCachedValue = true;
    }
    return yieldableState;
  }

  /**
   * if this method returns YieldableState.YIELDABLE, at least one of the cached field should be set
   */
  protected abstract YieldableState yieldValue() throws QueryProcessException, IOException;

  @Override
  public final void readyForNext() {
    hasCachedValue = false;
    currentNull = false;
  }

  @Override
  public final long currentTime() {
    return cachedTime;
  }

  @Override
  public final int currentInt() {
    return cachedInt;
  }

  @Override
  public final long currentLong() {
    return cachedLong;
  }

  @Override
  public final float currentFloat() {
    return cachedFloat;
  }

  @Override
  public final double currentDouble() {
    return cachedDouble;
  }

  @Override
  public final boolean currentBoolean() {
    return cachedBoolean;
  }

  @Override
  public final Binary currentBinary() {
    return cachedBinary;
  }

  @Override
  public final boolean isCurrentNull() {
    return currentNull;
  }
}
