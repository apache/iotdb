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
package org.apache.iotdb.db.mpp.plan.node.source;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.OrderBy;
import org.apache.iotdb.db.mpp.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * SeriesScanOperator is responsible for read data a specific series. When reading data, the
 * SeriesScanOperator can read the raw data batch by batch. And also, it can leverage the filter and
 * other info to decrease the result set.
 *
 * <p>Children type: no child is allowed for SeriesScanNode
 */
public class SeriesScanNode extends SourceNode {

  // The path of the target series which will be scanned.
  private PartialPath seriesPath;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  private OrderBy scanOrder = OrderBy.TIMESTAMP_ASC;

  // time filter for current series, could be null if doesn't exist
  private Filter timeFilter;

  // value filter for current series, could be null if doesn't exist
  private Filter valueFilter;

  // Limit for result set. The default value is -1, which means no limit
  private int limit;

  // offset for result set. The default value is 0
  private int offset;

  public SeriesScanNode(PlanNodeId id, PartialPath seriesPath) {
    super(id);
    this.seriesPath = seriesPath;
  }

  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  public void setValueFilter(Filter valueFilter) {
    this.valueFilter = valueFilter;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public void open() throws Exception {}

  public void setScanOrder(OrderBy scanOrder) {
    this.scanOrder = scanOrder;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }
}
