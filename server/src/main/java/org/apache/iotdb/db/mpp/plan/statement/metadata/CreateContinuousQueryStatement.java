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

import java.util.Collections;
import java.util.List;

public class CreateContinuousQueryStatement extends Statement implements IConfigStatement {

  private String cqId;
  private long everyInterval;
  private long boundaryTime;
  private long startTimeOffset;
  private long endTimeOffset;
  private TimeoutPolicy timeoutPolicy;
  private String queryBody;
  private String sql;

  public CreateContinuousQueryStatement() {
    super();
    statementType = StatementType.CREATE_CONTINUOUS_QUERY;
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

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }
}
