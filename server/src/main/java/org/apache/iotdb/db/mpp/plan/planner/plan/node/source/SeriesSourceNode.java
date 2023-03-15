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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public abstract class SeriesSourceNode extends SourceNode {

  // The path of the target series which will be scanned.
  protected final PartialPath seriesPath;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  protected Ordering scanOrder = Ordering.ASC;

  protected SeriesSourceNode(PlanNodeId id, PartialPath seriesPath) {
    super(id);
    this.seriesPath = seriesPath;
  }

  public PartialPath getPartitionPath() {
    return seriesPath;
  }

  public abstract Filter getPartitionTimeFilter();

  public Ordering getScanOrder() {
    return scanOrder;
  }

  public void setScanOrder(Ordering scanOrder) {
    this.scanOrder = scanOrder;
  }

  public PartialPath getSeriesPath() {
    return seriesPath;
  }
}
