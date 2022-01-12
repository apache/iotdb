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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class CreateContinuousQueryPlan extends PhysicalPlan {

  private String querySql;
  private String querySqlWithoutGroupByClause;
  private String continuousQueryName;
  private PartialPath targetPath;
  private long everyInterval;
  private long forInterval;
  private long groupByTimeInterval;
  private String groupByTimeIntervalString;
  private long creationTimestamp;

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
      String groupByTimeIntervalString) {
    super(Operator.OperatorType.CREATE_CONTINUOUS_QUERY);
    this.querySql = querySql;
    int indexOfGroupBy = querySql.indexOf("group by");
    this.querySqlWithoutGroupByClause =
        indexOfGroupBy == -1 ? querySql : querySql.substring(0, indexOfGroupBy);
    this.continuousQueryName = continuousQueryName;
    this.targetPath = targetPath;
    this.everyInterval = everyInterval;
    this.forInterval = forInterval;
    this.groupByTimeInterval = groupByTimeIntervalUnit;
    this.groupByTimeIntervalString = groupByTimeIntervalString;
    this.creationTimestamp = DatetimeUtils.currentTime();
  }

  public String getQuerySql() {
    return querySql;
  }

  public String getQuerySqlWithoutGroupByClause() {
    return querySqlWithoutGroupByClause;
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

  public long getCreationTimestamp() {
    return creationTimestamp;
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
    ReadWriteIOUtils.write(querySqlWithoutGroupByClause, buffer);
    ReadWriteIOUtils.write(targetPath.getFullPath(), buffer);
    buffer.putLong(everyInterval);
    buffer.putLong(forInterval);
    buffer.putLong(groupByTimeInterval);
    ReadWriteIOUtils.write(groupByTimeIntervalString, buffer);
    buffer.putLong(creationTimestamp);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    continuousQueryName = ReadWriteIOUtils.readString(buffer);
    querySql = ReadWriteIOUtils.readString(buffer);
    querySqlWithoutGroupByClause = ReadWriteIOUtils.readString(buffer);
    targetPath = new PartialPath(ReadWriteIOUtils.readString(buffer));
    everyInterval = ReadWriteIOUtils.readLong(buffer);
    forInterval = ReadWriteIOUtils.readLong(buffer);
    groupByTimeInterval = ReadWriteIOUtils.readLong(buffer);
    groupByTimeIntervalString = ReadWriteIOUtils.readString(buffer);
    creationTimestamp = ReadWriteIOUtils.readLong(buffer);
  }
}
