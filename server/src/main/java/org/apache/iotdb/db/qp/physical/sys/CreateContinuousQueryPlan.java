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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.*;

public class CreateContinuousQueryPlan extends PhysicalPlan {

  private String querySql;
  private String continuousQueryName;
  private PartialPath targetPath;
  private long everyInterval;
  private long forInterval;
  private QueryOperator queryOperator;
  private long creationTimestamp;

  public CreateContinuousQueryPlan() {
    super(false, Operator.OperatorType.CREATE_CONTINUOUS_QUERY);
  }

  public CreateContinuousQueryPlan(
      String querySql,
      String continuousQueryName,
      PartialPath targetPath,
      long everyInterval,
      long forInterval,
      QueryOperator queryOperator) {
    super(false, Operator.OperatorType.CREATE_CONTINUOUS_QUERY);
    this.querySql = querySql;
    this.continuousQueryName = continuousQueryName;
    this.targetPath = targetPath;
    this.everyInterval = everyInterval;
    this.forInterval = forInterval;
    this.queryOperator = queryOperator;
    this.creationTimestamp = DatetimeUtils.currentTime();
  }

  public void setQuerySql(String querySql) {
    this.querySql = querySql;
  }

  public String getQuerySql() {
    return querySql;
  }

  public void setContinuousQueryName(String continuousQueryName) {
    this.continuousQueryName = continuousQueryName;
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

  public void setQueryOperator(QueryOperator queryOperator) {
    this.queryOperator = queryOperator;
  }

  public QueryOperator getQueryOperator() {
    return queryOperator;
  }

  public void setCreationTimestamp(long creationTimestamp) {
    this.creationTimestamp = creationTimestamp;
  }

  public long getCreationTimestamp() {
    return creationTimestamp;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_CONTINUOUS_QUERY.ordinal());
    ReadWriteIOUtils.write(continuousQueryName, buffer);
    ReadWriteIOUtils.write(querySql, buffer);
    ReadWriteIOUtils.write(targetPath.getFullPath(), buffer);
    buffer.putLong(everyInterval);
    buffer.putLong(forInterval);
    buffer.putLong(creationTimestamp);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    continuousQueryName = ReadWriteIOUtils.readString(buffer);
    querySql = ReadWriteIOUtils.readString(buffer);
    targetPath = new PartialPath(ReadWriteIOUtils.readString(buffer));
    everyInterval = ReadWriteIOUtils.readLong(buffer);
    forInterval = ReadWriteIOUtils.readLong(buffer);
    creationTimestamp = ReadWriteIOUtils.readLong(buffer);
  }
}
