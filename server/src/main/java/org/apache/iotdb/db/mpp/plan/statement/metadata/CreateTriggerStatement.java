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

package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.enums.TriggerEvent;
import org.apache.iotdb.commons.trigger.enums.TriggerType;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CreateTriggerStatement extends Statement implements IConfigStatement {

  private final String triggerName;

  private final String className;

  private final TriggerEvent triggerEvent;

  private final TriggerType triggerType;

  private final PartialPath pathPattern;

  private final Map<String, String> attributes;

  public CreateTriggerStatement(
      String triggerName,
      String className,
      TriggerEvent triggerEvent,
      TriggerType triggerType,
      PartialPath pathPattern,
      Map<String, String> attributes) {
    this.triggerName = triggerName;
    this.className = className;
    this.triggerEvent = triggerEvent;
    this.triggerType = triggerType;
    this.pathPattern = pathPattern;
    this.attributes = attributes;
  }

  public String getTriggerName() {
    return triggerName;
  }

  public String getClassName() {
    return className;
  }

  public TriggerEvent getTriggerEvent() {
    return triggerEvent;
  }

  public TriggerType getTriggerType() {
    return triggerType;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }
}
