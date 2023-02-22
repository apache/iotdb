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

package org.apache.iotdb.db.mpp.plan.optimization;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;

public class PlanFactory {

  public static PlanNode scan(String id, PartialPath path) {
    return new SeriesScanNode(new PlanNodeId(id), (MeasurementPath) path);
  }

  public static PlanNode scan(String id, PartialPath path, int limit, int offset) {
    SeriesScanNode node = new SeriesScanNode(new PlanNodeId(id), (MeasurementPath) path);
    node.setLimit(limit);
    node.setOffset(offset);
    return node;
  }

  public static PlanNode limit(String id, long limit, PlanNode child) {
    return new LimitNode(new PlanNodeId(id), child, limit);
  }
}
