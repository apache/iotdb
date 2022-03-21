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

package org.apache.iotdb.db.query.udf.core.layer;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.common.expression.unary.ConstantOperand;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.core.reader.ConstantLayerPointReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowReader;
import org.apache.iotdb.db.query.udf.core.reader.LayerRowWindowReader;

/** IntermediateLayer for constants. */
public class ConstantIntermediateLayer extends IntermediateLayer {

  private final LayerPointReader constantLayerPointReaderCache;

  public ConstantIntermediateLayer(ConstantOperand expression, long queryId, float memoryBudgetInMB)
      throws QueryProcessException {
    super(expression, queryId, memoryBudgetInMB);
    constantLayerPointReaderCache = new ConstantLayerPointReader(expression);
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
}
