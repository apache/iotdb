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
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.trigger.service.TriggerManagementService;

import java.util.Collections;
import java.util.List;

public class DropTriggerStatement extends Statement implements IConfigStatement {
  private final String triggerName;

  private PartialPath authPath;

  public DropTriggerStatement(String triggerName) {
    super();
    statementType = StatementType.DROP_TRIGGER;
    this.triggerName = triggerName;
  }

  public String getTriggerName() {
    return triggerName;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitDropTrigger(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public boolean isAuthenticationRequired() {
    if (authPath == null) {
      TriggerInformation information =
          TriggerManagementService.getInstance().getTriggerInformation(triggerName);
      if (information == null) {
        return false;
      } else {
        authPath = information.getPathPattern();
      }
    }
    return true;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return isAuthenticationRequired()
        ? Collections.singletonList(authPath)
        : Collections.emptyList();
  }
}
