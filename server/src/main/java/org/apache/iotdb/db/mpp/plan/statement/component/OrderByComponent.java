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

import java.util.ArrayList;
import java.util.List;

/** The order of query result set */
public class OrderByComponent {

  private final List<SortItem> sortItemList;

  private boolean orderByTime = false;
  private boolean orderByTimeseries = false;
  private boolean orderByDevice = false;

  public OrderByComponent() {
    this.sortItemList = new ArrayList<>();
  }

  public void addSortItem(SortItem sortItem) {
    this.sortItemList.add(sortItem);

    if (sortItem.getSortKey() == SortItem.SortKey.TIME) {
      orderByTime = true;
    } else if (sortItem.getSortKey() == SortItem.SortKey.TIMESERIES) {
      orderByTimeseries = true;
    } else {
      orderByDevice = true;
    }
  }

  public List<SortItem> getSortItemList() {
    return sortItemList;
  }

  public boolean isOrderByTime() {
    return orderByTime;
  }

  public boolean isOrderByTimeseries() {
    return orderByTimeseries;
  }

  public boolean isOrderByDevice() {
    return orderByDevice;
  }
}
