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
 * Statement for showing user label policy in table model SHOW USER (username)? LABEL_POLICY FOR
 * (READ | WRITE | READ_WRITE)
 */
public class ShowTableUserLabelPolicyStatement extends Statement {

  private final String username;
  private final PolicyScope policyScope;

  public enum PolicyScope {
    READ,
    WRITE,
    READ_WRITE
  }

  public ShowTableUserLabelPolicyStatement(
      @Nullable NodeLocation location, String username, PolicyScope policyScope) {
    super(location);
    this.username = username;
    this.policyScope = policyScope;
  }

  public String getUsername() {
    return username;
  }

  public PolicyScope getPolicyScope() {
    return policyScope;
  }

  public StatementType getStatementType() {
    return StatementType.SHOW_TABLE_USER_LABEL_POLICY;
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, policyScope);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ShowTableUserLabelPolicyStatement that = (ShowTableUserLabelPolicyStatement) obj;
    return Objects.equals(username, that.username) && policyScope == that.policyScope;
  }

  @Override
  public String toString() {
    return String.format(
        "ShowTableUserLabelPolicyStatement{username='%s', policyScope='%s'}",
        username, policyScope);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowUserLabelPolicy(this, context);
  }
}
