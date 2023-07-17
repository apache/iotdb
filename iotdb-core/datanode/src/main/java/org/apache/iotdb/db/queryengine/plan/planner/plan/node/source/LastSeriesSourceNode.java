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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class LastSeriesSourceNode extends SeriesSourceNode {
  // The num of same series LastQueryScanNode on dispatched DataNode,
  // use reference value here to avoid iterate twice.
  private final AtomicInteger dataNodeSeriesScanNum;

  protected LastSeriesSourceNode(PlanNodeId id, AtomicInteger dataNodeSeriesScanNum) {
    super(id);
    this.dataNodeSeriesScanNum = dataNodeSeriesScanNum;
  }

  public AtomicInteger getDataNodeSeriesScanNum() {
    return dataNodeSeriesScanNum;
  }

  public abstract PartialPath getSeriesPath();

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    LastSeriesSourceNode that = (LastSeriesSourceNode) o;
    return dataNodeSeriesScanNum.get() == that.dataNodeSeriesScanNum.get();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), dataNodeSeriesScanNum);
  }
}
