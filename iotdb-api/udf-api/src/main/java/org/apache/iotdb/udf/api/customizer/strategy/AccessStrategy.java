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

package org.apache.iotdb.udf.api.customizer.strategy;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;

/**
 * Used to customize the strategy for accessing raw data in {@link UDTF#beforeStart(UDFParameters,
 * UDTFConfigurations)}.
 */
public interface AccessStrategy {

  enum AccessStrategyType {

    /** @see MappableRowByRowAccessStrategy */
    MAPPABLE_ROW_BY_ROW,

    /** @see RowByRowAccessStrategy */
    ROW_BY_ROW,

    /** @see SlidingTimeWindowAccessStrategy */
    SLIDING_TIME_WINDOW,

    /** @see SlidingSizeWindowAccessStrategy */
    SLIDING_SIZE_WINDOW,

    /** @see SessionTimeWindowAccessStrategy */
    SESSION_TIME_WINDOW,

    /** @see StateWindowAccessStrategy */
    STATE_WINDOW
  }

  /**
   * Used by the system to check the access strategy.
   *
   * @throws RuntimeException if invalid strategy is set
   */
  void check();

  /**
   * Returns the actual access strategy type.
   *
   * @return the actual access strategy type
   */
  AccessStrategyType getAccessStrategyType();
}
