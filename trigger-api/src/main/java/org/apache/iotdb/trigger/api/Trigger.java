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

package org.apache.iotdb.trigger.api;

import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.tsfile.write.record.Tablet;

public interface Trigger {

  /**
   * This method is mainly used to validate {@link TriggerAttributes} before calling {@link
   * Trigger#onCreate(TriggerAttributes)}.
   *
   * @param attributes TriggerAttributes
   * @throws Exception e
   */
  default void validate(TriggerAttributes attributes) throws Exception {}

  /**
   * This method will be called when creating a trigger after validation.
   *
   * @param attributes TriggerAttributes
   * @throws Exception e
   */
  default void onCreate(TriggerAttributes attributes) throws Exception {}

  /**
   * This method will be called when dropping a trigger.
   *
   * @throws Exception e
   */
  default void onDrop() throws Exception {}

  /**
   * When restarting a DataNode, Triggers that have been registered will be restored and this method
   * will be called during the process of restoring.
   *
   * @throws Exception e
   */
  default void restore() throws Exception {}

  /**
   * Overrides this method to set the expected FailureStrategy, {@link FailureStrategy#OPTIMISTIC}
   * is the default strategy.
   *
   * @return {@link FailureStrategy}
   */
  default FailureStrategy getFailureStrategy() {
    return FailureStrategy.OPTIMISTIC;
  }

  /**
   * @param tablet see {@link Tablet} for detailed information of data structure. Data that is
   *     inserted will be constructed as a Tablet and you can define process logic with {@link
   *     Tablet}.
   * @return true if successfully fired
   * @throws Exception e
   */
  default boolean fire(Tablet tablet) throws Exception {
    return true;
  }
}
