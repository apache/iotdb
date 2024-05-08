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

package org.apache.iotdb.db.mpp.transformation.dag.intermediate;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.LayerRowReader;
import org.apache.iotdb.db.mpp.transformation.api.LayerRowWindowReader;
import org.apache.iotdb.udf.api.customizer.strategy.AccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SessionTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.StateWindowAccessStrategy;

import java.io.IOException;

public abstract class IntermediateLayer {

  protected static final int CACHE_BLOCK_SIZE = 2;

  // for debug
  protected final Expression expression;

  protected final long queryId;
  protected final float memoryBudgetInMB;

  protected IntermediateLayer(Expression expression, long queryId, float memoryBudgetInMB) {
    this.expression = expression;
    this.queryId = queryId;
    this.memoryBudgetInMB = memoryBudgetInMB;
  }

  public abstract LayerPointReader constructPointReader();

  public abstract LayerRowReader constructRowReader();

  public final LayerRowWindowReader constructRowWindowReader(
      AccessStrategy strategy, float memoryBudgetInMB) throws QueryProcessException, IOException {
    switch (strategy.getAccessStrategyType()) {
      case SLIDING_TIME_WINDOW:
        return constructRowSlidingTimeWindowReader(
            (SlidingTimeWindowAccessStrategy) strategy, memoryBudgetInMB);
      case SLIDING_SIZE_WINDOW:
        return constructRowSlidingSizeWindowReader(
            (SlidingSizeWindowAccessStrategy) strategy, memoryBudgetInMB);
      case SESSION_TIME_WINDOW:
        return constructRowSessionTimeWindowReader(
            (SessionTimeWindowAccessStrategy) strategy, memoryBudgetInMB);
      case STATE_WINDOW:
        return constructRowStateWindowReader(
            (StateWindowAccessStrategy) strategy, memoryBudgetInMB);
      default:
        throw new IllegalStateException(
            "Unexpected access strategy: " + strategy.getAccessStrategyType());
    }
  }

  protected abstract LayerRowWindowReader constructRowSlidingSizeWindowReader(
      SlidingSizeWindowAccessStrategy strategy, float memoryBudgetInMB)
      throws QueryProcessException;

  protected abstract LayerRowWindowReader constructRowSlidingTimeWindowReader(
      SlidingTimeWindowAccessStrategy strategy, float memoryBudgetInMB)
      throws QueryProcessException, IOException;

  protected abstract LayerRowWindowReader constructRowSessionTimeWindowReader(
      SessionTimeWindowAccessStrategy strategy, float memoryBudgetInMB)
      throws QueryProcessException, IOException;

  protected abstract LayerRowWindowReader constructRowStateWindowReader(
      StateWindowAccessStrategy strategy, float memoryBudgetInMB)
      throws QueryProcessException, IOException;

  @Override
  public String toString() {
    return expression.toString();
  }
}
