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

import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.exception.TriggerExecutionException;
import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerExecutor {
  private final TriggerInformation triggerInformation;

  private final Trigger trigger;

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerExecutor.class);

  public TriggerExecutor(
      TriggerInformation triggerInformation, Trigger trigger, boolean isRestoring) {
    this.triggerInformation = triggerInformation;
    this.trigger = trigger;
    // call Trigger#validate and Trigger#onCreate
    onCreate();
    // Only call Trigger.restore() for stateful trigger
    if (isRestoring && triggerInformation.isStateful()) {
      onRestore();
    }
  }

  private void onCreate() {
    try {
      TriggerAttributes attributes = new TriggerAttributes(triggerInformation.getAttributes());
      trigger.validate(attributes);
      trigger.onCreate(attributes);
    } catch (Exception e) {
      onTriggerExecutionError("validate/onCreate(TriggerAttributes)", e);
    }
  }

  public void onDrop() {
    try {
      trigger.onDrop();
    } catch (Exception e) {
      onTriggerExecutionError("drop", e);
    }
  }

  private void onRestore() {
    try {
      trigger.restore();
    } catch (Exception e) {
      onTriggerExecutionError("restore", e);
    }
  }

  public boolean fire(Tablet tablet, TriggerEvent event) throws TriggerExecutionException {
    if (event.equals(triggerInformation.getEvent())) {
      try {
        return trigger.fire(tablet);
      } catch (Throwable t) {
        onTriggerExecutionError("fire(Tablet)", t);
      }
    } else {
      // !event.equals(triggerInformation.getEvent()) should not happen in normal case
      LOGGER.warn(
          "Trigger {} was fired with wrong event {}",
          triggerInformation.getTriggerName(),
          triggerInformation.getEvent());
    }
    return true;
  }

  public FailureStrategy getFailureStrategy() {
    return trigger.getFailureStrategy();
  }

  private void onTriggerExecutionError(String methodName, Throwable t)
      throws TriggerExecutionException {
    String errorMessage =
        String.format(
                "Error occurred during executing Trigger(%s)#%s: %s, perhaps need to check whether the implementation of Trigger is correct according to the trigger-api description.",
                triggerInformation.getTriggerName(), methodName, System.lineSeparator())
            + t;
    LOGGER.warn(errorMessage);
    throw new TriggerExecutionException(errorMessage);
  }

  public TriggerInformation getTriggerInformation() {
    return triggerInformation;
  }
}
