/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import java.util.Collections;
import java.util.List;

public class RemoveRedundantIdentityProjections implements RelationalPlanOptimizer {

  @Override
  public PlanNode optimize(
      PlanNode planNode,
      Analysis analysis,
      Metadata metadata,
      SessionInfo sessionInfo,
      MPPQueryContext context) {
    return planNode.accept(new Rewriter(), new RewriterContext());
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        context.setParent(node);
        newNode.addChild(child.accept(this, context));
      }
      return newNode;
    }

    @Override
    public PlanNode visitProject(ProjectNode projectNode, RewriterContext context) {
      // TODO change the impl using the method of context.getParent()
      if (projectNode.getChild() instanceof ProjectNode
          && projectNode.getOutputSymbols().equals(projectNode.getChild().getOutputSymbols())) {
        if (context.getParent() instanceof SingleChildProcessNode) {
          ((SingleChildProcessNode) context.getParent()).setChild(projectNode.getChild());
        } else {
          List<PlanNode> children = context.getParent().getChildren();
          for (int i = 0; i < children.size(); i++) {
            PlanNode child = children.get(i);
            if (child.getPlanNodeId().equals(projectNode.getPlanNodeId())) {
              Collections.swap(children, i, children.size() - 1);
              children.remove(children.size() - 1);
              break;
            }
          }
        }
        return projectNode.getChild().accept(this, context);
      } else {
        projectNode.getChild().accept(this, context);
        return projectNode;
      }
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriterContext context) {
      return node;
    }
  }

  private static class RewriterContext {
    private PlanNode parent;

    public RewriterContext() {}

    public PlanNode getParent() {
      return this.parent;
    }

    public void setParent(PlanNode parent) {
      this.parent = parent;
    }
  }
}
