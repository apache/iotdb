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
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.LayerRowReader;
import org.apache.iotdb.db.mpp.transformation.api.LayerRowWindowReader;
import org.apache.iotdb.db.mpp.transformation.dag.input.ConstantInputReader;
import org.apache.iotdb.udf.api.customizer.strategy.SessionTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.StateWindowAccessStrategy;

/** IntermediateLayer for constants. */
public class ConstantIntermediateLayer extends IntermediateLayer {

  private final LayerPointReader constantLayerPointReaderCache;

  public ConstantIntermediateLayer(ConstantOperand expression, long queryId, float memoryBudgetInMB)
      throws QueryProcessException {
    super(expression, queryId, memoryBudgetInMB);
    constantLayerPointReaderCache = new ConstantInputReader(expression);
  }

  @Override
  public LayerPointReader constructPointReader() {
    return constantLayerPointReaderCache;
  }

  @Override
  public LayerRowReader constructRowReader() {
    // Not allowed since the timestamp of a constant row is not defined.
    throw new UnsupportedOperationException();
  }

  @Override
  protected LayerRowWindowReader constructRowSlidingSizeWindowReader(
      SlidingSizeWindowAccessStrategy strategy, float memoryBudgetInMB) {
    // Not allowed since the timestamp of a constant row is not defined.
    throw new UnsupportedOperationException();
  }

  @Override
  protected LayerRowWindowReader constructRowSlidingTimeWindowReader(
      SlidingTimeWindowAccessStrategy strategy, float memoryBudgetInMB) {
    // Not allowed since the timestamp of a constant row is not defined.
    throw new UnsupportedOperationException();
  }

  @Override
  protected LayerRowWindowReader constructRowSessionTimeWindowReader(
      SessionTimeWindowAccessStrategy strategy, float memoryBudgetInMB) {
    // Not allowed since the timestamp of a constant row is not defined.
    throw new UnsupportedOperationException();
  }

  @Override
  protected LayerRowWindowReader constructRowStateWindowReader(
      StateWindowAccessStrategy strategy, float memoryBudgetInMB) {
    // Not allowed since the timestamp of a constant row is not defined.
    throw new UnsupportedOperationException(
        "StateWindowAccessStrategy does not support pure constant input.");
  }
}
