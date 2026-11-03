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

package org.apache.iotdb.db.trigger.executor;

import org.apache.iotdb.db.i18n.DataNodeMiscMessages;

public enum TriggerFireResult {
  SUCCESS(0, DataNodeMiscMessages.EXCEPTION_TRIGGER_FIRES_SUCCESSFULLY_FACD095E),

  FAILED_NO_TERMINATION(
      1,
      DataNodeMiscMessages
          .EXCEPTION_EXCEPTION_OCCURRED_WHEN_FIRING_AN_TRIGGER_WITH_OPTIMISTIC_STRATEGY_623857A6),

  TERMINATION(
      2,
      DataNodeMiscMessages
          .EXCEPTION_EXCEPTION_OCCURRED_WHEN_FIRING_AN_TRIGGER_WITH_PESSIMISTIC_STRATEGY_ED052363);

  private final int id;
  private final String event;

  TriggerFireResult(int id, String event) {
    this.id = id;
    this.event = event;
  }

  public static TriggerFireResult construct(int id) {
    switch (id) {
      case 0:
        return SUCCESS;
      case 1:
        return FAILED_NO_TERMINATION;
      case 2:
        return TERMINATION;
      default:
        throw new UnsupportedOperationException(
            DataNodeMiscMessages.UNSUPPORTED_TRIGGER_FIRE_RESULT_TYPE);
    }
  }

  public int getId() {
    return id;
  }

  public String getEvent() {
    return event;
  }
}
