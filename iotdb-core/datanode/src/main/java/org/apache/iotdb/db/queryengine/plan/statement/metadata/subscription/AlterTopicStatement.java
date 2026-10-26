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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AlterTopicStatement extends Statement implements IConfigStatement {

  private String topicName;
  private Map<String, String> topicAttributes;

  public AlterTopicStatement() {
    super();
    statementType = StatementType.ALTER_TOPIC;
  }

  public String getTopicName() {
    return topicName;
  }

  public Map<String, String> getTopicAttributes() {
    return topicAttributes;
  }

  public void setTopicName(final String topicName) {
    this.topicName = topicName;
  }

  public void setTopicAttributes(final Map<String, String> topicAttributes) {
    this.topicAttributes = topicAttributes;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.OTHER;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(final StatementVisitor<R, C> visitor, final C context) {
    return visitor.visitAlterTopic(this, context);
  }
}
