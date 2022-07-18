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

package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class StartTriggerPlan extends PhysicalPlan {

  private String triggerName;

  private PartialPath authPath;

  public StartTriggerPlan() {
    super(OperatorType.START_TRIGGER);
    canBeSplit = false;
  }

  public StartTriggerPlan(String triggerName) {
    super(OperatorType.START_TRIGGER);
    this.triggerName = triggerName;
    canBeSplit = false;
  }

  public String getTriggerName() {
    return triggerName;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.START_TRIGGER.ordinal());

    putString(stream, triggerName);
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.START_TRIGGER.ordinal());

    putString(buffer, triggerName);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    triggerName = readString(buffer);
  }

  @Override
  public boolean isAuthenticationRequired() {
    if (authPath == null) {
      try {
        authPath =
            TriggerRegistrationService.getInstance()
                .getRegistrationInformation(triggerName)
                .getFullPath();
      } catch (TriggerManagementException e) {
        // The trigger does not exist.
        return false;
      }
    }
    return true;
  }

  @Override
  public List<? extends PartialPath> getAuthPaths() {
    return isAuthenticationRequired()
        ? Collections.singletonList(authPath)
        : Collections.emptyList();
  }
}
