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

package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.utils.DateTimeUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class CreateContinuousQueryPlan extends PhysicalPlan {

  private String querySql;
  private String querySqlBeforeGroupByClause;
  private String querySqlAfterGroupByClause;
  private String continuousQueryName;
  private PartialPath targetPath;
  private long everyInterval;
  private long forInterval;
  private long groupByTimeInterval;
  private String groupByTimeIntervalString;
  private long firstExecutionTimeBoundary;

  public CreateContinuousQueryPlan() {
    super(Operator.OperatorType.CREATE_CONTINUOUS_QUERY);
  }

  public CreateContinuousQueryPlan(
      String querySql,
      String continuousQueryName,
      PartialPath targetPath,
      long everyInterval,
      long forInterval,
      long groupByTimeIntervalUnit,
      String groupByTimeIntervalString,
      Long firstExecutionTimeBoundary) {
    super(Operator.OperatorType.CREATE_CONTINUOUS_QUERY);
    this.querySql = querySql;
    String querySqlLowerCase = querySql.toLowerCase();
    int indexOfGroupBy = querySqlLowerCase.indexOf("group by");
    this.querySqlBeforeGroupByClause =
        indexOfGroupBy == -1 ? querySql : querySql.substring(0, indexOfGroupBy);
    int indexOfLevel = querySqlLowerCase.indexOf("level");
    this.querySqlAfterGroupByClause = indexOfLevel == -1 ? "" : querySql.substring(indexOfLevel);
    this.continuousQueryName = continuousQueryName;
    this.targetPath = targetPath;
    this.everyInterval = everyInterval;
    this.forInterval = forInterval;
    this.groupByTimeInterval = groupByTimeIntervalUnit;
    this.groupByTimeIntervalString = groupByTimeIntervalString;
    this.firstExecutionTimeBoundary =
        firstExecutionTimeBoundary != null
            ? firstExecutionTimeBoundary
            : DateTimeUtils.currentTime();
  }

  public String getQuerySql() {
    return querySql;
  }

  public String getQuerySqlBeforeGroupByClause() {
    return querySqlBeforeGroupByClause;
  }

  public String getQuerySqlAfterGroupByClause() {
    return querySqlAfterGroupByClause;
  }

  public String getContinuousQueryName() {
    return continuousQueryName;
  }

  public void setTargetPath(PartialPath targetPath) {
    this.targetPath = targetPath;
  }

  public PartialPath getTargetPath() {
    return targetPath;
  }

  public long getEveryInterval() {
    return everyInterval;
  }

  public long getForInterval() {
    return forInterval;
  }

  public long getGroupByTimeInterval() {
    return groupByTimeInterval;
  }

  public String getGroupByTimeIntervalString() {
    return groupByTimeIntervalString;
  }

  public long getFirstExecutionTimeBoundary() {
    return firstExecutionTimeBoundary;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_CONTINUOUS_QUERY.ordinal());
    ReadWriteIOUtils.write(continuousQueryName, buffer);
    ReadWriteIOUtils.write(querySql, buffer);
    ReadWriteIOUtils.write(querySqlBeforeGroupByClause, buffer);
    ReadWriteIOUtils.write(querySqlAfterGroupByClause, buffer);
    ReadWriteIOUtils.write(targetPath.getFullPath(), buffer);
    buffer.putLong(everyInterval);
    buffer.putLong(forInterval);
    buffer.putLong(groupByTimeInterval);
    ReadWriteIOUtils.write(groupByTimeIntervalString, buffer);
    buffer.putLong(firstExecutionTimeBoundary);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    continuousQueryName = ReadWriteIOUtils.readString(buffer);
    querySql = ReadWriteIOUtils.readString(buffer);
    querySqlBeforeGroupByClause = ReadWriteIOUtils.readString(buffer);
    querySqlAfterGroupByClause = ReadWriteIOUtils.readString(buffer);
    targetPath = new PartialPath(ReadWriteIOUtils.readString(buffer));
    everyInterval = ReadWriteIOUtils.readLong(buffer);
    forInterval = ReadWriteIOUtils.readLong(buffer);
    groupByTimeInterval = ReadWriteIOUtils.readLong(buffer);
    groupByTimeIntervalString = ReadWriteIOUtils.readString(buffer);
    firstExecutionTimeBoundary = ReadWriteIOUtils.readLong(buffer);
  }
}
