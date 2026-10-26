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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowStatement;

import java.util.Collections;
import java.util.List;

public class ShowDiskUsageStatement extends ShowStatement {
  private final PartialPath pathPattern;
  private WhereCondition whereCondition;
  private OrderByComponent orderByComponent;

  public ShowDiskUsageStatement(PartialPath pathPattern) {
    this.statementType = StatementType.SHOW_DISK_USAGE;
    this.pathPattern = pathPattern;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
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

  public List<SortItem> getSortItemList() {
    return orderByComponent == null ? Collections.emptyList() : orderByComponent.getSortItemList();
  }

  @Override
  public boolean isQuery() {
    return true;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowDiskUsage(this, context);
  }
}
