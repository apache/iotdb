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

package org.apache.iotdb.db.engine.trigger.service;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.TriggerEvent;
import org.apache.iotdb.commons.trigger.TriggerOperationType;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StartTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StopTriggerPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * TODO refactor Trigger TriggerRegistrationInformation to TriggerManagementInfo and optimize its
 * lifecycle
 */
public class TriggerRegistrationInformation {

  private final String triggerName;
  private TriggerEvent event = null;
  private PartialPath fullPath = null;
  private String className = null;
  private Map<String, String> attributes = null;

  private volatile boolean isStopped;
  private final TriggerOperationType operationType;

  private TriggerRegistrationInformation(String triggerName, TriggerOperationType operationType) {
    this.triggerName = triggerName;
    this.operationType = operationType;
  }

  public static TriggerRegistrationInformation getCreateInfo(
      String triggerName,
      TriggerEvent event,
      PartialPath fullPath,
      String className,
      Map<String, String> attributes) {
    TriggerRegistrationInformation registrationInformation =
        new TriggerRegistrationInformation(triggerName, TriggerOperationType.CREATE);
    registrationInformation.setEvent(event);
    registrationInformation.setFullPath(fullPath);
    registrationInformation.setClassName(className);
    registrationInformation.setAttributes(attributes);
    return registrationInformation;
  }

  public static TriggerRegistrationInformation getDropInfo(String triggerName) {
    return new TriggerRegistrationInformation(triggerName, TriggerOperationType.DROP);
  }

  public static TriggerRegistrationInformation getStartInfo(String triggerName) {
    return new TriggerRegistrationInformation(triggerName, TriggerOperationType.START);
  }

  public static TriggerRegistrationInformation getStopInfo(String triggerName) {
    return new TriggerRegistrationInformation(triggerName, TriggerOperationType.STOP);
  }

  public void markAsStarted() {
    isStopped = false;
  }

  public void markAsStopped() {
    isStopped = true;
  }

  public String getTriggerName() {
    return triggerName;
  }

  public TriggerEvent getEvent() {
    return event;
  }

  private void setEvent(TriggerEvent event) {
    this.event = event;
  }

  public PartialPath getFullPath() {
    return fullPath;
  }

  private void setFullPath(PartialPath fullPath) {
    this.fullPath = fullPath;
  }

  public String getClassName() {
    return className;
  }

  private void setClassName(String className) {
    this.className = className;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  private void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public boolean isStopped() {
    return isStopped;
  }

  public TriggerOperationType getOperationType() {
    return operationType;
  }

  public void serialize(ByteBuffer buffer) throws IOException {
    buffer.mark();
    try {
      if (TriggerOperationType.CREATE == operationType) {
        buffer.put((byte) TriggerOperationType.CREATE.ordinal());
        ReadWriteIOUtils.write(triggerName, buffer);
        buffer.put(event.getId());
        ReadWriteIOUtils.write(fullPath.getFullPath(), buffer);
        ReadWriteIOUtils.write(className, buffer);
        ReadWriteIOUtils.write(attributes, buffer);
      } else if (TriggerOperationType.DROP == operationType) {
        buffer.put((byte) TriggerOperationType.DROP.ordinal());
        ReadWriteIOUtils.write(triggerName, buffer);
      } else if (TriggerOperationType.START == operationType) {
        buffer.put((byte) TriggerOperationType.START.ordinal());
        ReadWriteIOUtils.write(triggerName, buffer);
      } else if (TriggerOperationType.STOP == operationType) {
        buffer.put((byte) TriggerOperationType.STOP.ordinal());
        ReadWriteIOUtils.write(triggerName, buffer);
      }
    } catch (UnsupportedOperationException e) {
      // ignore and throw
      throw e;
    } catch (Exception e) {
      buffer.reset();
      throw e;
    }
  }

  public PhysicalPlan convertToPhysicalPlan() throws TriggerManagementException {
    switch (operationType) {
      case CREATE:
        return new CreateTriggerPlan(triggerName, event, fullPath, className, attributes);
      case DROP:
        return new DropTriggerPlan(triggerName);
      case START:
        return new StartTriggerPlan(triggerName);
      case STOP:
        return new StopTriggerPlan(triggerName);
      default:
        throw new TriggerManagementException(
            "TriggerRegistrationInformation converts to PhysicalPlan error");
    }
  }

  public static TriggerRegistrationInformation convertFromPhysicalPlan(PhysicalPlan plan) {
    switch (plan.getOperatorType()) {
      case CREATE_TRIGGER:
        CreateTriggerPlan createTriggerPlan = (CreateTriggerPlan) plan;
        return getCreateInfo(
            createTriggerPlan.getTriggerName(),
            createTriggerPlan.getEvent(),
            createTriggerPlan.getFullPath(),
            createTriggerPlan.getClassName(),
            createTriggerPlan.getAttributes());
      case DROP_TRIGGER:
        return getDropInfo(((DropTriggerPlan) plan).getTriggerName());
      case START_TRIGGER:
        return getStartInfo(((StartTriggerPlan) plan).getTriggerName());
      case STOP_TRIGGER:
        return getStopInfo(((StopTriggerPlan) plan).getTriggerName());
      default:
        return null;
    }
  }
}
