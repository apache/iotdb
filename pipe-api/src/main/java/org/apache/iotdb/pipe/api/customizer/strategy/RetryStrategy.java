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

package org.apache.iotdb.pipe.api.customizer.strategy;

public interface RetryStrategy {
  enum RetryStrategyType {

    /** @see SimpleRetryStrategy */
    SIMPLE,

    /** @see ExponentialBackOffStrategy */
    EXPONENTIAL_BACK_OFF
  }

  /**
   * Used by the system to check the retry strategy.
   *
   * @throws RuntimeException if invalid strategy is set
   */
  void check();

  /**
   * Returns the actual retry strategy type.
   *
   * @return the actual retry strategy type
   */
  RetryStrategyType getRetryStrategyType();
}
