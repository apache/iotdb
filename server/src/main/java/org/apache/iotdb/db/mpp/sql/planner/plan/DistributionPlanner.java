/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.sql.planner.plan;

import org.apache.iotdb.db.mpp.common.Analysis;
import org.apache.iotdb.db.mpp.common.DataRegion;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.SimplePlanNodeRewriter;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;

import java.util.*;

public class DistributionPlanner {
    private Analysis analysis;
    private LogicalQueryPlan logicalPlan;

    public DistributionPlanner(Analysis analysis, LogicalQueryPlan logicalPlan) {
        this.analysis = analysis;
        this.logicalPlan = logicalPlan;
    }

    public PlanNode rewriteSource() {
        SourceRewriter rewriter = new SourceRewriter();
        return rewriter.visit(logicalPlan.getRootNode(), new DistributionPlanContext());
    }

    public DistributedQueryPlan planFragments() {
        return null;
    }

    private class SourceRewriter extends SimplePlanNodeRewriter<DistributionPlanContext> {
        public PlanNode visitTimeJoin(TimeJoinNode node, DistributionPlanContext context) {
            TimeJoinNode root = (TimeJoinNode) node.clone();

            // Step 1: Get all source nodes. For the node which is not source, add it as the child of current TimeJoinNode
            List<SeriesScanNode> sources = new ArrayList<>();
            for (PlanNode child : node.getChildren()) {
                if (child instanceof SeriesScanNode) {
                    // If the child is SeriesScanNode, we need to check whether this node should be seperated into several splits.
                    SeriesScanNode handle = (SeriesScanNode) child;
                    Set<DataRegion> dataDistribution = analysis.getPartitionInfo(handle.getSeriesPath(), handle.getTimeFilter());
                    // If the size of dataDistribution is m, this SeriesScanNode should be seperated into m SeriesScanNode.
                    for (DataRegion dataRegion : dataDistribution) {
                        SeriesScanNode split = (SeriesScanNode) handle.clone();
                        split.setDataRegion(dataRegion);
                        sources.add(split);
                    }
                } else if (child instanceof SeriesAggregateScanNode) {
                    //TODO: (xingtanzjr) We should do the same thing for SeriesAggregateScanNode. Consider to make SeriesAggregateScanNode
                    // and SeriesScanNode to derived from the same parent Class because they have similar process logic in many scenarios
                } else {
                    // In a general logical query plan, the children of TimeJoinNode should only be SeriesScanNode or SeriesAggregateScanNode
                    // So this branch should not be touched.
                    root.addChild(visit(child, context));
                }
            }

            // Step 2: For the source nodes, group them by the DataRegion.
            Map<DataRegion, List<SeriesScanNode>> sourceGroup = new HashMap<>();
            sources.forEach(source -> {
                List<SeriesScanNode> group = sourceGroup.containsKey(source.getDataRegion()) ?
                        sourceGroup.get(source.getDataRegion()) : new ArrayList<>();
                group.add(source);
                sourceGroup.put(source.getDataRegion(), group);
            });

            // Step 3: For the source nodes which belong to same data region, add a TimeJoinNode for them and make the
            // new TimeJoinNode as the child of current TimeJoinNode
            sourceGroup.forEach((dataRegion, seriesScanNodes) -> {
                if (seriesScanNodes.size() == 1) {
                    root.addChild(seriesScanNodes.get(0));
                } else {
                    // We clone a TimeJoinNode from root to make the params to be consistent
                    TimeJoinNode parentOfGroup = (TimeJoinNode) root.clone();
                    seriesScanNodes.forEach(parentOfGroup::addChild);
                    root.addChild(parentOfGroup);
                }
            });

            return root;
        }

        public PlanNode visit(PlanNode node, DistributionPlanContext context) {
            return node.accept(this, context);
        }
    }

    private class DistributionPlanContext {

    }
}
