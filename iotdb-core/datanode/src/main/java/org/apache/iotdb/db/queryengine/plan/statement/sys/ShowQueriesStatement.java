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

package org.apache.iotdb.db.queryengine.plan.statement.sys;

import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowStatement;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

public class ShowQueriesStatement extends ShowStatement {

  private WhereCondition whereCondition;

  private OrderByComponent orderByComponent;

  private long rowLimit;
  private long rowOffset;

  private ZoneId zoneId;

  public ShowQueriesStatement() {
    // do nothing
  }

  @Override
  public boolean isQuery() {
    return true;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowQueries(this, context);
  }

  public void setWhereCondition(WhereCondition whereCondition) {
    this.whereCondition = whereCondition;
  }

  public WhereCondition getWhereCondition() {
    return whereCondition;
  }

  public void setOrderByComponent(OrderByComponent orderByComponent) {
    this.orderByComponent = orderByComponent;
  }

  public OrderByComponent getOrderByComponent() {
    return orderByComponent;
  }

  public List<SortItem> getSortItemList() {
    if (orderByComponent == null) {
      // default order
      return Collections.singletonList(new SortItem(OrderByKey.TIME, Ordering.ASC));
    }
    return orderByComponent.getSortItemList();
  }

  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  public long getRowLimit() {
    return rowLimit;
  }

  public void setRowOffset(long rowOffset) {
    this.rowOffset = rowOffset;
  }

  public long getRowOffset() {
    return rowOffset;
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
  }
}
