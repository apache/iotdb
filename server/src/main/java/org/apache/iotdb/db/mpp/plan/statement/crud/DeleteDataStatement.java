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

package org.apache.iotdb.db.mpp.plan.statement.crud;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.util.List;

public class DeleteDataStatement extends Statement {

  private List<PartialPath> pathList;
  private long deleteStartTime;
  private long deleteEndTime;

  public DeleteDataStatement() {
    super();
    statementType = StatementType.DELETE;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return pathList;
  }

  public List<PartialPath> getPathList() {
    return pathList;
  }

  public void setPathList(List<PartialPath> pathList) {
    this.pathList = pathList;
  }

  public long getDeleteStartTime() {
    return deleteStartTime;
  }

  public void setDeleteStartTime(long deleteStartTime) {
    this.deleteStartTime = deleteStartTime;
  }

  public long getDeleteEndTime() {
    return deleteEndTime;
  }

  public void setDeleteEndTime(long deleteEndTime) {
    this.deleteEndTime = deleteEndTime;
  }

  public void setTimeRange(TimeRange timeRange) {
    this.deleteStartTime = timeRange.getMin();
    this.deleteEndTime = timeRange.getMax();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitDeleteData(this, context);
  }
}
