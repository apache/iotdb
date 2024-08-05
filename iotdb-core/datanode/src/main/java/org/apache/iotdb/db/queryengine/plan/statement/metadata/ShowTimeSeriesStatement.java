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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTreeUtils;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;

/**
 * SHOW TIMESERIES statement.
 *
 * <p>Here is the syntax definition:
 *
 * <p>SHOW [LATEST] TIMESERIES [pathPattern] [WHERE key { = | CONTAINS } value] [LIMIT limit]
 * [OFFSET offset]
 */
public class ShowTimeSeriesStatement extends ShowStatement {

  private final PartialPath pathPattern;
  private SchemaFilter schemaFilter;
  // if is true, the result will be sorted according to the inserting frequency of the time series
  private final boolean orderByHeat;
  private WhereCondition timeCondition;
  private OrderByComponent orderByComponent = null;

  public ShowTimeSeriesStatement(PartialPath pathPattern, boolean orderByHeat) {
    super();
    this.pathPattern = pathPattern;
    this.orderByHeat = orderByHeat;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
  }

  public void setSchemaFilter(SchemaFilter schemaFilter) {
    this.schemaFilter = schemaFilter;
  }

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  public void setTimeCondition(WhereCondition timeCondition) {
    this.timeCondition = timeCondition;
  }

  public WhereCondition getTimeCondition() {
    return timeCondition;
  }

  public boolean hasTimeCondition() {
    return timeCondition != null;
  }

  public void setOrderByComponent(OrderByComponent orderByComponent) {
    this.orderByComponent = orderByComponent;
  }

  public OrderByComponent getOrderByComponent() {
    return orderByComponent;
  }

  public boolean hasOrderByComponent() {
    return orderByComponent != null;
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    if (hasTimeCondition()) {
      try {
        if (!AuthorityChecker.SUPER_USER.equals(userName)) {
          this.authorityScope =
              PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                  AuthorityChecker.getAuthorizedPathTree(
                      userName, PrivilegeType.READ_SCHEMA.ordinal()),
                  AuthorityChecker.getAuthorizedPathTree(
                      userName, PrivilegeType.READ_DATA.ordinal()));
        }
      } catch (AuthException e) {
        return new TSStatus(e.getCode().getStatusCode());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return super.checkPermissionBeforeProcess(userName);
    }
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(pathPattern);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowTimeSeries(this, context);
  }
}
