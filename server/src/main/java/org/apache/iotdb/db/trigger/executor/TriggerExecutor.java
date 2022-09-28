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

public class TriggerExecutor {
  private final TriggerInformation triggerInformation;

  private final Trigger trigger;

  public TriggerExecutor(TriggerInformation triggerInformation, Trigger trigger) {
    this.triggerInformation = triggerInformation;
    this.trigger = trigger;
    // call Trigger#validate and Trigger#onCreate
    onCreate();
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

  private void onTriggerExecutionError(String methodName, Exception e)
      throws TriggerExecutionException {
    throw new TriggerExecutionException(
        String.format(
                "Error occurred during executing Trigger#%s: %s",
                methodName, System.lineSeparator())
            + e);
  }
}
