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
package org.apache.iotdb.trigger.api.enums;

import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.tsfile.write.record.Tablet;

/**
 * Type of FailureStrategy indicates what will happen when a trigger failed to do {@link
 * Trigger#fire(Tablet)}. The triggers will be fired sequentially before and after the insertion.
 * For example: Trigger1(BEFORE_INSERT) -> Trigger2(BEFORE_INSERT) -> Insertion ->
 * Trigger3(AFTER_INSERT) -> Trigger4(AFTER_INSERT)
 */
public enum FailureStrategy {
  /**
   * If this strategy were adopted, the failure of {@link Trigger#fire(Tablet)} of one Tablet would
   * not have any influence on the triggers that have not been fired. The failure of this Trigger
   * will be simply ignored.
   */
  OPTIMISTIC(0),

  /**
   * If this strategy were adopted, the failure of {@link Trigger#fire(Tablet)} of one Tablet
   * would @@ -45,5 +45,26 @@ public enum FailureStrategy { an insertion, all the triggers that have
   * not fired will not be fired, and this insertion will be marked as failed even if the insertion
   * itself executed successfully.
   */
  PESSIMISTIC(1);

  private final int id;

  FailureStrategy(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }

  public static FailureStrategy construct(int id) {
    switch (id) {
      case 0:
        return FailureStrategy.OPTIMISTIC;
      case 1:
        return FailureStrategy.PESSIMISTIC;
      default:
        throw new UnsupportedOperationException("Unsupported FailureStrategy Type.");
    }
  }
}
