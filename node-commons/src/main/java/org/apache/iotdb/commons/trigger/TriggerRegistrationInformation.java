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

package org.apache.iotdb.commons.trigger;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class TriggerRegistrationInformation {

  private String triggerName;
  private TriggerEvent event = null;
  private PartialPath fullPath = null;
  private String className = null;
  private Map<String, String> attributes = null;

  private volatile boolean isStopped;
  private TriggerManagementType managementType;

  private TriggerRegistrationInformation() {}

  private TriggerRegistrationInformation(String triggerName, TriggerManagementType managementType) {
    this.triggerName = triggerName;
    this.managementType = managementType;
  }

  public static TriggerRegistrationInformation getCreateInfo(
      String triggerName,
      TriggerEvent event,
      PartialPath fullPath,
      String className,
      Map<String, String> attributes) {
    TriggerRegistrationInformation registrationInformation =
        new TriggerRegistrationInformation(triggerName, TriggerManagementType.CREATE);
    registrationInformation.setEvent(event);
    registrationInformation.setFullPath(fullPath);
    registrationInformation.setClassName(className);
    registrationInformation.setAttributes(attributes);
    return registrationInformation;
  }

  public static TriggerRegistrationInformation getDropInfo(String triggerName) {
    return new TriggerRegistrationInformation(triggerName, TriggerManagementType.DROP);
  }

  public static TriggerRegistrationInformation getStartInfo(String triggerName) {
    return new TriggerRegistrationInformation(triggerName, TriggerManagementType.START);
  }

  public static TriggerRegistrationInformation getStopInfo(String triggerName) {
    return new TriggerRegistrationInformation(triggerName, TriggerManagementType.STOP);
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

  public TriggerManagementType getManagementType() {
    return managementType;
  }

  public void serialize(ByteBuffer buffer) {
    buffer.mark();
    buffer.put((byte) managementType.ordinal());
    ReadWriteIOUtils.write(triggerName, buffer);
    if (TriggerManagementType.CREATE == managementType) {
      ReadWriteIOUtils.write(triggerName, buffer);
      buffer.put(event.getId());
      ReadWriteIOUtils.write(fullPath.getFullPath(), buffer);
      ReadWriteIOUtils.write(className, buffer);
      ReadWriteIOUtils.write(attributes, buffer);
    }
  }

  public void deserialize(ByteBuffer buffer) throws IOException, IllegalPathException {
    int typeNum = buffer.get();
    if (typeNum < 0 || typeNum >= TriggerManagementType.values().length) {
      throw new IOException("unrecognized log type: " + typeNum);
    }
    managementType = TriggerManagementType.values()[typeNum];
    triggerName = ReadWriteIOUtils.readString(buffer);
    if (TriggerManagementType.CREATE == managementType) {
      triggerName = ReadWriteIOUtils.readString(buffer);
      event = TriggerEvent.construct(buffer.get());
      fullPath = new PartialPath(ReadWriteIOUtils.readString(buffer));
      className = ReadWriteIOUtils.readString(buffer);
      attributes = ReadWriteIOUtils.readMap(buffer);
    }
  }

  public static TriggerRegistrationInformation createFromBuffer(ByteBuffer buffer)
      throws IllegalPathException, IOException {
    TriggerRegistrationInformation info = new TriggerRegistrationInformation();
    info.deserialize(buffer);
    return info;
  }
}
