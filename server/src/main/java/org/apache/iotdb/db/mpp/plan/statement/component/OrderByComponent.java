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

package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.db.mpp.plan.statement.StatementNode;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/** The order of query result set */
public class OrderByComponent extends StatementNode {

  private final List<SortItem> sortItemList;

  private boolean orderByTime = false;
  private int timeOrderPriority = -1;

  private boolean orderByTimeseries = false;
  private int timeseriesOrderPriority = -1;

  private boolean orderByDevice = false;
  private int deviceOrderPriority = -1;

  public OrderByComponent() {
    this.sortItemList = new ArrayList<>();
  }

  public void addSortItem(SortItem sortItem) {
    this.sortItemList.add(sortItem);
    if (sortItem.getSortKey() == SortKey.TIME) {
      orderByTime = true;
      timeOrderPriority = sortItemList.size() - 1;
    } else if (sortItem.getSortKey() == SortKey.TIMESERIES) {
      orderByTimeseries = true;
      timeseriesOrderPriority = sortItemList.size() - 1;
    } else {
      orderByDevice = true;
      deviceOrderPriority = sortItemList.size() - 1;
    }
  }

  public List<SortItem> getSortItemList() {
    return sortItemList;
  }

  public boolean isOrderByTime() {
    return orderByTime;
  }

  public Ordering getTimeOrder() {
    checkState(timeOrderPriority != -1, "The time order is not specified.");
    return sortItemList.get(timeOrderPriority).getOrdering();
  }

  public boolean isOrderByTimeseries() {
    return orderByTimeseries;
  }

  public Ordering getTimeseriesOrder() {
    checkState(timeOrderPriority != -1, "The timeseries order is not specified.");
    return sortItemList.get(timeseriesOrderPriority).getOrdering();
  }

  public boolean isOrderByDevice() {
    return orderByDevice;
  }

  public Ordering getDeviceOrder() {
    checkState(deviceOrderPriority != -1, "The device order is not specified.");
    return sortItemList.get(deviceOrderPriority).getOrdering();
  }

  public String toSQLString() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("ORDER BY ");
    for (int i = 0; i < sortItemList.size(); i++) {
      sqlBuilder.append(sortItemList.get(i).toSQLString());
      if (i < sortItemList.size() - 1) {
        sqlBuilder.append(", ");
      }
    }
    return sqlBuilder.toString();
  }
}
