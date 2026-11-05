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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class OptimizerRuleStatsTest {

  @Test
  public void testRuleStatisticsAreCollectedOnQueryContext() {
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan plan =
        planTester.createPlan("select tag1, count(*) from table1 group by tag1");

    MPPQueryContext queryContext = plan.getContext();
    PlanOptimizersStatsCollector collector = queryContext.getPlanOptimizersStatsCollector();
    assertNotNull(collector);
    // The same collector is shared by the logical and the distributed optimizers.
    assertSame(collector, queryContext.getOrCreatePlanOptimizersStatsCollector());
    assertFalse(collector.getTopRuleStats().isEmpty());
  }
}
