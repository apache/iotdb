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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.tsfile.CopyToTsFileOptions;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CopyToNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;

import com.google.common.collect.ImmutableList;

/** Removes an inner sort superseded by the TAG/TIME sort required for COPY TO TsFile. */
public class RemoveRedundantSortForCopyTo implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    if (!context.getAnalysis().hasSortNode()) {
      return plan;
    }
    return plan.accept(new Rewriter(), null);
  }

  private static class Rewriter implements PlanVisitor<PlanNode, Void> {

    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
      // COPY TO is a statement, so only its output and EXPLAIN ANALYZE wrappers can precede it.
      if (node instanceof OutputNode || node instanceof ExplainAnalyzeNode) {
        PlanNode child = node.getChildren().get(0);
        PlanNode rewrittenChild = child.accept(this, context);
        return child == rewrittenChild
            ? node
            : node.replaceChildren(ImmutableList.of(rewrittenChild));
      }
      return node;
    }

    @Override
    public PlanNode visitCopyTo(CopyToNode node, Void context) {
      if (node.getCopyToOptions() instanceof CopyToTsFileOptions options
          && !options.isGenerateNewTimeColumn()
          && node.getChild() instanceof SortNode sortNode) {
        PlanNode source = removeRedundantSort(sortNode.getChild());
        if (source == sortNode.getChild()) {
          return node;
        }
        return node.replaceChildren(
            ImmutableList.of(sortNode.replaceChildren(ImmutableList.of(source))));
      }
      return node;
    }

    private PlanNode removeRedundantSort(PlanNode node) {
      if (node instanceof SortNode sortNode) {
        return sortNode.getChild();
      }
      // Stop at LIMIT, OFFSET and order-dependent computations; only identity projections are safe.
      if (node instanceof ProjectNode projectNode && projectNode.isIdentity()) {
        PlanNode child = removeRedundantSort(projectNode.getChild());
        return child == projectNode.getChild()
            ? node
            : projectNode.replaceChildren(ImmutableList.of(child));
      }
      return node;
    }
  }
}
