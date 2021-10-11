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

package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.engine.trigger.executor.TriggerEvent;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.util.HashMap;
import java.util.Map;

public class CreateTriggerOperator extends Operator {

  private String triggerName;
  private TriggerEvent event;
  private PartialPath fullPath;
  private String className;
  private final Map<String, String> attributes;

  public CreateTriggerOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.CREATE_TRIGGER;
    attributes = new HashMap<>();
  }

  public void setTriggerName(String triggerName) {
    this.triggerName = triggerName;
  }

  public void setEvent(TriggerEvent event) {
    this.event = event;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public void setFullPath(PartialPath fullPath) {
    this.fullPath = fullPath;
  }

  public void addAttribute(String key, String value) {
    attributes.put(key, value);
  }

  public String getTriggerName() {
    return triggerName;
  }

  public TriggerEvent getEvent() {
    return event;
  }

  public String getClassName() {
    return className;
  }

  public PartialPath getFullPath() {
    return fullPath;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new CreateTriggerPlan(triggerName, event, fullPath, className, attributes);
  }
}
