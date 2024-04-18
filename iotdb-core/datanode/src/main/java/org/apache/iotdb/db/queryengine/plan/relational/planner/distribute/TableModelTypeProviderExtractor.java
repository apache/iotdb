package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.SimplePlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import java.util.HashMap;

public class TableModelTypeProviderExtractor {
  public static TypeProvider extractor(PlanNode root, TypeProvider allTypes) {
    TypeProvider typeProvider = new TypeProvider(new HashMap<>());
    root.accept(new Visitor(typeProvider, allTypes), null);
    return typeProvider;
  }

  private static class Visitor extends SimplePlanVisitor<Void> {
    private final TypeProvider typeProvider;
    private final TypeProvider allTypes;

    public Visitor(TypeProvider typeProvider, TypeProvider allTypes) {
      this.typeProvider = typeProvider;
      this.allTypes = allTypes;
    }

    @Override
    public Void visitPlan(PlanNode node, Void context) {
      node.getOutputSymbols()
          .forEach(
              symbol -> typeProvider.putTableModelType(symbol, allTypes.getTableModelType(symbol)));
      for (PlanNode source : node.getChildren()) {
        source.accept(this, context);
      }
      return null;
    }

    @Override
    public Void visitTableScan(TableScanNode node, Void context) {
      node.getAssignments().forEach((k, v) -> typeProvider.putTableModelType(k, v.getType()));
      return null;
    }

    @Override
    public Void visitProject(ProjectNode node, Void context) {
      node.getChild().accept(this, context);
      // TODO add expression process logic
      //            node.getAssignments().forEach((k,v) -> typeProvider.putTableModelType(k,
      //                    v.getType()));
      return null;
    }

    @Override
    public Void visitFilter(FilterNode node, Void context) {
      node.getChild().accept(this, context);
      return null;
    }

    @Override
    public Void visitOutput(OutputNode node, Void context) {
      node.getChild().accept(this, context);
      return null;
    }
  }
}
