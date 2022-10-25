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

import org.apache.iotdb.commons.cq.TimeoutPolicy;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;

import java.util.Collections;
import java.util.List;

public class CreateContinuousQueryStatement extends Statement implements IConfigStatement {

  private String sql;
  private String cqId;

  // The query execution time interval, default value is group_by_interval in group by clause.
  private long everyInterval;

  // A date that represents the execution time of a certain cq task, default value is 0.
  private long boundaryTime = 0;

  // The start time of each query execution, default value is every_interval
  private long startTimeOffset;

  // The end time of each query execution, default value is 0.
  private long endTimeOffset = 0;

  // Specify how we deal with the cq task whose previous time interval execution is not finished
  // while the next execution time has reached, default value is BLOCKED.
  private TimeoutPolicy timeoutPolicy = TimeoutPolicy.BLOCKED;

  private String queryBody;
  private QueryStatement queryBodyStatement;

  public CreateContinuousQueryStatement() {
    super();
    statementType = StatementType.CREATE_CONTINUOUS_QUERY;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getCqId() {
    return cqId;
  }

  public void setCqId(String cqId) {
    this.cqId = cqId;
  }

  public long getEveryInterval() {
    return everyInterval;
  }

  public void setEveryInterval(long everyInterval) {
    this.everyInterval = everyInterval;
  }

  public long getBoundaryTime() {
    return boundaryTime;
  }

  public void setBoundaryTime(long boundaryTime) {
    this.boundaryTime = boundaryTime;
  }

  public long getStartTimeOffset() {
    return startTimeOffset;
  }

  public void setStartTimeOffset(long startTimeOffset) {
    this.startTimeOffset = startTimeOffset;
  }

  public long getEndTimeOffset() {
    return endTimeOffset;
  }

  public void setEndTimeOffset(long endTimeOffset) {
    this.endTimeOffset = endTimeOffset;
  }

  public TimeoutPolicy getTimeoutPolicy() {
    return timeoutPolicy;
  }

  public void setTimeoutPolicy(TimeoutPolicy timeoutPolicy) {
    this.timeoutPolicy = timeoutPolicy;
  }

  public String getQueryBody() {
    return queryBody;
  }

  public void setQueryBody(String queryBody) {
    this.queryBody = queryBody;
  }

  public QueryStatement getQueryBodyStatement() {
    return queryBodyStatement;
  }

  public void setQueryBodyStatement(QueryStatement queryBodyStatement) {
    this.queryBodyStatement = queryBodyStatement;
  }

  public String getZoneId() {
    return queryBodyStatement.getSelectComponent().getZoneId().getId();
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateContinuousQuery(this, context);
  }
}
