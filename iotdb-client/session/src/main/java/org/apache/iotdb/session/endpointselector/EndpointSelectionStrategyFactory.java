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

package org.apache.iotdb.session.endpointselector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndpointSelectionStrategyFactory {

  private static final Logger logger =
      LoggerFactory.getLogger(EndpointSelectionStrategyFactory.class);

  public static final String ENDPOINT_SELECTION_STRATEGY_SEQUENTIAL = "sequential";
  public static final String ENDPOINT_SELECTION_STRATEGY_RANDOM = "random";

  private EndpointSelectionStrategyFactory() {
    // Private constructor to prevent instantiation
  }

  /**
   * Creates an endpoint selection strategy based on the given strategy name.
   *
   * @param strategyName name of the strategy ("random" or "sequential")
   * @return configured strategy instance
   * @throws IllegalArgumentException if strategyName is null or empty
   */
  public static EndpointSelectionStrategy createSelectionStrategy(String strategyName) {
    if (strategyName == null || strategyName.trim().isEmpty()) {
      throw new IllegalArgumentException("Strategy name cannot be null or empty");
    }

    String normalizedName = strategyName.trim().toLowerCase();
    switch (normalizedName) {
      case ENDPOINT_SELECTION_STRATEGY_SEQUENTIAL:
        return new SequentialSelectionStrategy();
      case ENDPOINT_SELECTION_STRATEGY_RANDOM:
        return new RandomSelectionStrategy();
      default:
        logger.warn(
            "Unknown endpoint selection strategy: '{}'. Defaulting to random strategy.",
            strategyName);
        return new RandomSelectionStrategy();
    }
  }
}
