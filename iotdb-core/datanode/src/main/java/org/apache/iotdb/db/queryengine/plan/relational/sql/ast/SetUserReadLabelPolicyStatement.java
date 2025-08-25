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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.queryengine.plan.statement.StatementType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Statement for setting user read label policy in table model ALTER USER username SET LABEL_POLICY
 * 'expression' FOR READ
 */
public class SetUserReadLabelPolicyStatement extends Statement {

  private final String username;
  private final String policyExpression;

  public SetUserReadLabelPolicyStatement(
      @Nullable NodeLocation location, String username, String policyExpression) {
    super(location);
    this.username = username;
    this.policyExpression = policyExpression;
  }

  public String getUsername() {
    return username;
  }

  public String getPolicyExpression() {
    return policyExpression;
  }

  public StatementType getStatementType() {
    return StatementType.SET_TABLE_USER_READ_LABEL_POLICY;
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, policyExpression);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SetUserReadLabelPolicyStatement that = (SetUserReadLabelPolicyStatement) obj;
    return Objects.equals(username, that.username)
        && Objects.equals(policyExpression, that.policyExpression);
  }

  @Override
  public String toString() {
    return String.format(
        "SetUserReadLabelPolicyStatement{username='%s', policyExpression='%s'}",
        username, policyExpression);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSetUserReadLabelPolicy(this, context);
  }
}
