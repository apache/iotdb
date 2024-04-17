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
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.relational.sql.tree.Expression;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;

/** Extract IDeviceID and */
public class IndexScan implements RelationalPlanOptimizer {

  @Override
  public PlanNode optimize(
      PlanNode planNode,
      Analysis analysis,
      Metadata metadata,
      SessionInfo sessionInfo,
      MPPQueryContext context) {
    return planNode.accept(new Rewriter(), new RewriterContext(null, metadata, sessionInfo));
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {
    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      for (PlanNode child : node.getChildren()) {
        child.accept(this, context);
      }
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriterContext context) {
      context.setPredicate(node.getPredicate());
      return node;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriterContext context) {
      List<String> attributeColumns =
          node.getAssignments().entrySet().stream()
              .filter(e -> e.getValue().getColumnCategory().equals(ATTRIBUTE))
              .map(e -> e.getKey().getName())
              .collect(Collectors.toList());
      // TODO extract predicate to expression list
      List<DeviceEntry> deviceEntries =
          context
              .getMetadata()
              .indexScan(
                  new QualifiedObjectName(
                      context.getSessionInfo().getDatabaseName().get(),
                      node.getQualifiedTableName()),
                  Collections.singletonList(context.getPredicate()),
                  attributeColumns);
      node.setDeviceEntries(deviceEntries);
      return node;
    }
  }

  private static class RewriterContext {
    private Expression predicate;
    private Metadata metadata;
    private final SessionInfo sessionInfo;

    RewriterContext(Expression predicate, Metadata metadata, SessionInfo sessionInfo) {
      this.predicate = predicate;
      this.metadata = metadata;
      this.sessionInfo = sessionInfo;
    }

    public Expression getPredicate() {
      return this.predicate;
    }

    public void setPredicate(Expression predicate) {
      this.predicate = predicate;
    }

    public Metadata getMetadata() {
      return this.metadata;
    }

    public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
    }

    public SessionInfo getSessionInfo() {
      return this.sessionInfo;
    }
  }
}
