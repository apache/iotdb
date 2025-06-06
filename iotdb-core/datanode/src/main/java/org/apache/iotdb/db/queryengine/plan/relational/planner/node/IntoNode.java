package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IntoNode extends SingleChildProcessNode {
  private final QualifiedName table;
  private final List<Symbol> outputSymbols;
  private final List<String> columnNames;

  public IntoNode(
      PlanNodeId id,
      PlanNode child,
      QualifiedName table,
      List<Symbol> outputSymbols,
      List<String> columnNames) {
    super(id, child);
    this.table = table;
    this.outputSymbols = ImmutableList.of(new Symbol("rows"));
    // this.outputSymbols = ImmutableList.of(new Symbol("time"), new Symbol("name"), new
    // Symbol("salary"));
    this.columnNames = columnNames;
  }

  public PlanNode getChild() {
    return child;
  }

  public void setChild(PlanNode child) {
    this.child = child;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.singletonList(child);
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public void addChild(PlanNode child) {
    this.child = child;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInto(this, context);
  }

  @Override
  public PlanNode clone() {
    return new IntoNode(id, null, table, outputSymbols, columnNames);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputSymbols.stream().map(Symbol::getName).collect(Collectors.toList());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

  @Override
  public List<Symbol> getOutputSymbols() {
    return outputSymbols;
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new IntoNode(
        id, Iterables.getOnlyElement(newChildren), table, outputSymbols, columnNames);
  }

  @Override
  public String toString() {
    return "IntoNode-" + this.getPlanNodeId();
  }

  //  @Override
  //  public boolean equals(Object o) {
  //  }
  //
  //  @Override
  //  public int hashCode() {
  //  }
}
