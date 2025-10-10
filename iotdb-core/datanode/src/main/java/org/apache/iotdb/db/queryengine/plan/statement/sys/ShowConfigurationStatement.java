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

package org.apache.iotdb.db.queryengine.plan.statement.sys;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ShowConfigurationStatement extends Statement implements IConfigStatement {

  private final boolean showAllConfigurations;
  private final int nodeId;
  private final boolean withDescription;
  private Collection<PrivilegeType> missingPrivileges;

  public ShowConfigurationStatement(
      boolean showAllConfigurations, int nodeId, boolean withDescription) {
    this.statementType = StatementType.SHOW_CONFIGURATION;
    this.showAllConfigurations = showAllConfigurations;
    this.nodeId = nodeId;
    this.withDescription = withDescription;
  }

  public Collection<PrivilegeType> getMissingPrivileges() {
    return missingPrivileges;
  }

  public void setMissingPrivileges(Collection<PrivilegeType> missingPrivileges) {
    this.missingPrivileges = missingPrivileges;
  }

  public boolean isShowAllConfigurations() {
    return showAllConfigurations;
  }

  public int getNodeId() {
    return nodeId;
  }

  public boolean withDescription() {
    return withDescription;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowConfiguration(this, context);
  }
}
