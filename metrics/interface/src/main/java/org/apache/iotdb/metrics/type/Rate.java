/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.type;

public interface Rate extends IMetric {
  /**
   * Get value of Rate
   * @return
   */
  long getCount();

  /**
   * Get one minute rate
   * @return
   */
  double getOneMinuteRate();

  /**
   * Get mean rate
   * @return
   */
  double getMeanRate();

  /**
   * Get five minute rate
   * @return
   */
  double getFiveMinuteRate();

  /**
   * Get fifteen minute rate
   * @return
   */
  double getFifteenMinuteRate();

  /**
   * mark in rate
   */
  void mark();

  /**
   * mark n in rate
   * @param n
   */
  void mark(long n);
}
