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

package org.apache.iotdb.db.queryengine.plan.statement.sys.quota;

import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SetThrottleQuotaStatement extends Statement implements IConfigStatement {

  private String userName;
  private Map<ThrottleType, TTimedQuota> throttleLimit;
  private long memLimit;
  private int cpuLimit;

  /** QuotaOperator Constructor with OperatorType. */
  public SetThrottleQuotaStatement() {
    super();
    statementType = StatementType.SET_THROTTLE_QUOTA;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public Map<ThrottleType, TTimedQuota> getThrottleLimit() {
    return throttleLimit;
  }

  public void setThrottleLimit(Map<ThrottleType, TTimedQuota> throttleLimit) {
    this.throttleLimit = throttleLimit;
  }

  public long getMemLimit() {
    return memLimit;
  }

  public void setMemLimit(long memLimit) {
    this.memLimit = memLimit;
  }

  public int getCpuLimit() {
    return cpuLimit;
  }

  public void setCpuLimit(int cpuLimit) {
    this.cpuLimit = cpuLimit;
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
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitSetThrottleQuota(this, context);
  }
}
