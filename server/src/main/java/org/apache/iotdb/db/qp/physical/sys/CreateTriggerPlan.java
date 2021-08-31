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

import org.apache.iotdb.db.engine.trigger.executor.TriggerEvent;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class CreateTriggerPlan extends PhysicalPlan {

  private String triggerName;
  private TriggerEvent event;
  private PartialPath fullPath;
  private String className;
  private Map<String, String> attributes;

  /**
   * This field is mainly used for the stage of recovering trigger registration information, so it
   * will never be serialized into a log file.
   *
   * <p>Note that the status of triggers registered by executing SQL statements is STARTED by
   * default, so this field should be {@code false} by default.
   *
   * @see TriggerRegistrationService
   */
  private boolean isStopped = false;

  public CreateTriggerPlan() {
    super(false, OperatorType.CREATE_TRIGGER);
    canBeSplit = false;
  }

  public CreateTriggerPlan(
      String triggerName,
      TriggerEvent event,
      PartialPath fullPath,
      String className,
      Map<String, String> attributes) {
    super(false, OperatorType.CREATE_TRIGGER);
    this.triggerName = triggerName;
    this.event = event;
    this.fullPath = fullPath;
    this.className = className;
    this.attributes = attributes;
    canBeSplit = false;
  }

  public String getTriggerName() {
    return triggerName;
  }

  public TriggerEvent getEvent() {
    return event;
  }

  public PartialPath getFullPath() {
    return fullPath;
  }

  public String getClassName() {
    return className;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public boolean isStopped() {
    return isStopped;
  }

  public void markAsStarted() {
    isStopped = false;
  }

  public void markAsStopped() {
    isStopped = true;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.CREATE_TRIGGER.ordinal());

    putString(stream, triggerName);
    stream.write(event.getId());
    putString(stream, fullPath.getFullPath());
    putString(stream, className);

    stream.write(attributes.size());
    for (Entry<String, String> attribute : attributes.entrySet()) {
      putString(stream, attribute.getKey());
      putString(stream, attribute.getValue());
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_TRIGGER.ordinal());

    putString(buffer, triggerName);
    buffer.put(event.getId());
    putString(buffer, fullPath.getFullPath());
    putString(buffer, className);

    buffer.putInt(attributes.size());
    for (Entry<String, String> attribute : attributes.entrySet()) {
      putString(buffer, attribute.getKey());
      putString(buffer, attribute.getValue());
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    triggerName = readString(buffer);
    event = TriggerEvent.construct(buffer.get());
    fullPath = new PartialPath(readString(buffer));
    className = readString(buffer);

    attributes = new HashMap<>();
    int attributeNumber = buffer.getInt();
    for (int i = 0; i < attributeNumber; ++i) {
      String key = readString(buffer);
      String value = readString(buffer);
      attributes.put(key, value);
    }
  }
}
