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
 * Statement for dropping user label policy in table model ALTER USER username DROP LABEL_POLICY FOR
 * (READ | WRITE | READ,WRITE)
 */
public class DropUserLabelPolicyStatement extends Statement {

  private final String username;
  private final String scope;

  public DropUserLabelPolicyStatement(
      @Nullable NodeLocation location, String username, String scope) {
    super(location);
    this.username = username;
    this.scope = scope;
  }

  public String getUsername() {
    return username;
  }

  public String getScope() {
    return scope;
  }

  public String getStatementType() {
    return "DROP_USER_LABEL_POLICY";
  }

  @Override
  public List<? extends org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node>
      getChildren() {
    return Collections.emptyList();
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, scope);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    DropUserLabelPolicyStatement that = (DropUserLabelPolicyStatement) obj;
    return Objects.equals(username, that.username) && Objects.equals(scope, that.scope);
  }

  @Override
  public String toString() {
    return String.format(
        "DropUserLabelPolicyStatement{username='%s', scope='%s'}", username, scope);
  }
}
