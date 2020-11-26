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

package org.apache.iotdb.db.query.udf.api.customizer.strategy;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;

/**
 * Used in {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
 * <p>
 * When the access strategy of a UDTF is set to an instance of this class, the method {@link
 * UDTF#transform(RowWindow, PointCollector)} of the UDTF will be called to transform the original
 * data. You need to override the method in your own UDTF class.
 * <p>
 * Sliding size window is a kind of size-based window. Except for the last call, each call of the
 * method {@link UDTF#transform(RowWindow, PointCollector)} processes a window with {@code
 * windowSize} rows (aligned by time) of the original data and can generate any number of data
 * points.
 * <p>
 * Sample code:
 * <pre>{@code
 * @Override
 * public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
 *   configurations
 *       .setOutputDataType(TSDataType.INT32)
 *       .setAccessStrategy(new SlidingSizeWindowAccessStrategy(10000)); // window size
 * }</pre>
 *
 * @see UDTF
 * @see UDTFConfigurations
 */
public class SlidingSizeWindowAccessStrategy implements AccessStrategy {

  private final int windowSize;

  /**
   * Constructor. You need to specify the number of rows in each sliding size window (except for the
   * last window).
   *
   * @param windowSize the number of rows in each sliding size window (0 < windowSize)
   */
  public SlidingSizeWindowAccessStrategy(int windowSize) {
    this.windowSize = windowSize;
  }

  @Override
  public void check() throws QueryProcessException {
    if (windowSize <= 0) {
      throw new QueryProcessException(
          String.format("Parameter windowSize(%d) should be positive.", windowSize));
    }
  }

  public int getWindowSize() {
    return windowSize;
  }

  @Override
  public AccessStrategyType getAccessStrategyType() {
    return AccessStrategyType.SLIDING_SIZE_WINDOW;
  }
}
