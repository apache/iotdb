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
 * software distributed under this work for additional information
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
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.auth.AuthorityChecker.SUCCEED;

/**
 * ShowUserLabelPolicyStatement represents the SQL statement: SHOW USER [username] LABEL_POLICY FOR
 * READ|WRITE|READ,WRITE
 *
 * <p>This statement is used to show label policies for users to support Label-Based Access Control
 * (LBAC) functionality.
 */
public class ShowUserLabelPolicyStatement extends Statement implements IConfigStatement {

  /** The username to show label policy for, null means show all users */
  private String username;

  /** The scope of the label policy: READ, WRITE, or READ,WRITE */
  private LabelPolicyScope scope;

  public ShowUserLabelPolicyStatement() {
    super();
    this.statementType = StatementType.SHOW_USER_LABEL_POLICY;
  }

  public ShowUserLabelPolicyStatement(String username, LabelPolicyScope scope) {
    this();
    this.username = username;
    this.scope = scope;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public LabelPolicyScope getScope() {
    return scope;
  }

  public void setScope(LabelPolicyScope scope) {
    this.scope = scope;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowUserLabelPolicy(this, context);
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    // Only root administrator can view label policies
    if (!"root".equalsIgnoreCase(userName)) {
      return RpcUtils.getStatus(
          TSStatusCode.NO_PERMISSION.getStatusCode(),
          "Only root administrator can view label policies.");
    }
    return SUCCEED;
  }

  /** Label policy scope enum */
  public enum LabelPolicyScope {
    READ,
    WRITE,
    READ_WRITE,
    WRITE_READ
  }
}
