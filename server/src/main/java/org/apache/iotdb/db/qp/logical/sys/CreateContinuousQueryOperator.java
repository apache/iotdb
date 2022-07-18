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

package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class CreateContinuousQueryOperator extends Operator {

  private String querySql;
  private QueryOperator queryOperator;
  private String continuousQueryName;
  private PartialPath targetPath;
  private long everyInterval;
  private long forInterval;
  private long groupByTimeInterval;
  private String groupByTimeIntervalString;
  private Long firstExecutionTimeBoundary;

  public CreateContinuousQueryOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.CREATE_CONTINUOUS_QUERY;
  }

  public void setQuerySql(String querySql) {
    this.querySql = querySql;
  }

  public void setContinuousQueryName(String continuousQueryName) {
    this.continuousQueryName = continuousQueryName;
  }

  public void setTargetPath(PartialPath targetPath) {
    this.targetPath = targetPath;
  }

  public void setEveryInterval(long everyInterval) {
    this.everyInterval = everyInterval;
  }

  public long getEveryInterval() {
    return everyInterval;
  }

  public void setForInterval(long forInterval) {
    this.forInterval = forInterval;
  }

  public long getForInterval() {
    return forInterval;
  }

  public void setGroupByTimeInterval(long groupByTimeInterval) {
    this.groupByTimeInterval = groupByTimeInterval;
  }

  public void setGroupByTimeIntervalString(String groupByTimeIntervalString) {
    this.groupByTimeIntervalString = groupByTimeIntervalString;
  }

  public void setFirstExecutionTimeBoundary(long firstExecutionTimeBoundary) {
    this.firstExecutionTimeBoundary = firstExecutionTimeBoundary;
  }

  public void setQueryOperator(QueryOperator queryOperator) {
    this.queryOperator = queryOperator;
  }

  public QueryOperator getQueryOperator() {
    return queryOperator;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new CreateContinuousQueryPlan(
        querySql,
        continuousQueryName,
        targetPath,
        everyInterval,
        forInterval,
        groupByTimeInterval,
        groupByTimeIntervalString,
        firstExecutionTimeBoundary);
  }
}
