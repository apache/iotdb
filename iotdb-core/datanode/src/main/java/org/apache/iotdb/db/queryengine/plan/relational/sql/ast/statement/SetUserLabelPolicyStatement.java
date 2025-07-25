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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast.statement;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Statement for setting user label policy in table model ALTER USER username SET LABEL_POLICY
 * 'expression' FOR (READ | WRITE | READ,WRITE)
 */
public class SetUserLabelPolicyStatement extends Statement {

  private final String username;
  private final String policyExpression;
  private final String scope;

  public SetUserLabelPolicyStatement(
      @Nullable NodeLocation location, String username, String policyExpression, String scope) {
    super(location);
    this.username = username;
    this.policyExpression = policyExpression;
    this.scope = scope;
  }

  public String getUsername() {
    return username;
  }

  public String getPolicyExpression() {
    return policyExpression;
  }

  public String getScope() {
    return scope;
  }

  public String getStatementType() {
    return "SET_USER_LABEL_POLICY";
  }

  @Override
  public List<? extends org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node>
      getChildren() {
    return Collections.emptyList();
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, policyExpression, scope);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SetUserLabelPolicyStatement that = (SetUserLabelPolicyStatement) obj;
    return Objects.equals(username, that.username)
        && Objects.equals(policyExpression, that.policyExpression)
        && Objects.equals(scope, that.scope);
  }

  @Override
  public String toString() {
    return String.format(
        "SetUserLabelPolicyStatement{username='%s', policyExpression='%s', scope='%s'}",
        username, policyExpression, scope);
  }
}
