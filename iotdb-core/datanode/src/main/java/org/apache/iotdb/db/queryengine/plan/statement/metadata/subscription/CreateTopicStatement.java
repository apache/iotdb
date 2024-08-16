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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CreateTopicStatement extends Statement implements IConfigStatement {

  private String topicName;
  private boolean ifNotExistsCondition;
  private Map<String, String> topicAttributes;

  public CreateTopicStatement() {
    super();
    statementType = StatementType.CREATE_TOPIC;
  }

  public String getTopicName() {
    return topicName;
  }

  public boolean hasIfNotExistsCondition() {
    return ifNotExistsCondition;
  }

  public Map<String, String> getTopicAttributes() {
    return topicAttributes;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public void setIfNotExists(boolean ifNotExistsCondition) {
    this.ifNotExistsCondition = ifNotExistsCondition;
  }

  public void setTopicAttributes(Map<String, String> topicAttributes) {
    this.topicAttributes = topicAttributes;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.USE_PIPE.ordinal()),
        PrivilegeType.USE_PIPE);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTopic(this, context);
  }
}
