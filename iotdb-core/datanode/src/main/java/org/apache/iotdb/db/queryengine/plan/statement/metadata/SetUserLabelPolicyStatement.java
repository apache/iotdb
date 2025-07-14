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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.auth.AuthorityChecker.SUCCEED;

/**
 * SetUserLabelPolicyStatement represents the SQL statement: ALTER USER username SET LABEL_POLICY
 * <策略表达式> FOR READ|WRITE|READ,WRITE
 *
 * <p>This statement is used to set or update label policies for users to support Label-Based Access
 * Control (LBAC) functionality.
 */
public class SetUserLabelPolicyStatement extends Statement implements IConfigStatement {

  /** The username to set label policy for */
  private String username;

  /** The policy expression to set */
  private String policyExpression;

  /** The scope of the label policy: READ, WRITE, or READ,WRITE */
  private ShowUserLabelPolicyStatement.LabelPolicyScope scope;

  public SetUserLabelPolicyStatement() {
    super();
    this.statementType = StatementType.SET_USER_LABEL_POLICY;
  }

  public SetUserLabelPolicyStatement(
      String username,
      String policyExpression,
      ShowUserLabelPolicyStatement.LabelPolicyScope scope) {
    this();
    this.username = username;
    this.policyExpression = policyExpression;
    this.scope = scope;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPolicyExpression() {
    return policyExpression;
  }

  public void setPolicyExpression(String policyExpression) {
    this.policyExpression = policyExpression;
  }

  public ShowUserLabelPolicyStatement.LabelPolicyScope getScope() {
    return scope;
  }

  public void setScope(ShowUserLabelPolicyStatement.LabelPolicyScope scope) {
    this.scope = scope;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitSetUserLabelPolicy(this, context);
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    return SUCCEED;
  }
}
