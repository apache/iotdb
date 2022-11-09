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

package org.apache.iotdb.db.mpp.plan.statement.sys.quota;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

public class SetSpaceQuotaStatement extends Statement implements IConfigStatement {

  private int timeSeriesNum;
  private int deviceNum;
  private long diskSize;
  private String policy;
  private List<String> prefixPathList;

  /** QuotaOperator Constructor with OperatorType. */
  public SetSpaceQuotaStatement() {
    super();
    statementType = StatementType.SET_SPACE_QUOTA;
  }

  public int getTimeSeriesNum() {
    return timeSeriesNum;
  }

  public void setTimeSeriesNum(int timeSeriesNum) {
    this.timeSeriesNum = timeSeriesNum;
  }

  public int getDeviceNum() {
    return deviceNum;
  }

  public void setDeviceNum(int deviceNum) {
    this.deviceNum = deviceNum;
  }

  public long getDiskSize() {
    return diskSize;
  }

  public void setDiskSize(long diskSize) {
    this.diskSize = diskSize;
  }

  public String getPolicy() {
    return policy;
  }

  public void setPolicy(String policy) {
    this.policy = policy;
  }

  public List<String> getPrefixPathList() {
    return prefixPathList;
  }

  public void setPrefixPathList(List<String> prefixPathList) {
    this.prefixPathList = prefixPathList;
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
    return visitor.visitSetSpaceQuota(this, context);
  }
}
