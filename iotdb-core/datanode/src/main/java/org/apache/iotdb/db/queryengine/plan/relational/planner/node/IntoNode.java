package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;

import com.google.common.collect.Iterables;
import org.apache.tsfile.read.common.type.Type;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IntoNode extends SingleChildProcessNode {
  private final QualifiedName table;
  private final List<ColumnSchema> tableColumns;
  private final List<Symbol> outputSymbols;
  private final List<Type> outputTypes;
  private final int outputSize;

  public IntoNode(
      PlanNodeId id,
      PlanNode child,
      QualifiedName table,
      List<ColumnSchema> tableColumns,
      List<Field> outputFields) {
    super(id, child);
    this.table = table;
    this.tableColumns = tableColumns;
    this.outputSize = outputFields.size();
    this.outputSymbols = new ArrayList<>();
    this.outputTypes = new ArrayList<>();
    for (Field field : outputFields) {
      outputSymbols.add(Symbol.of(field.getName().orElse("")));
      outputTypes.add(field.getType());
    }
  }

  public IntoNode(
      PlanNodeId id,
      PlanNode child,
      QualifiedName table,
      List<ColumnSchema> tableColumns,
      int outputSize,
      List<Symbol> outputSymbols,
      List<Type> outputTypes) {
    super(id, child);
    this.table = table;
    this.tableColumns = tableColumns;
    this.outputSize = outputSize;
    this.outputSymbols = outputSymbols;
    this.outputTypes = outputTypes;
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
    return new IntoNode(id, null, table, tableColumns, outputSize, outputSymbols, outputTypes);
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
        id,
        Iterables.getOnlyElement(newChildren),
        table,
        tableColumns,
        outputSize,
        outputSymbols,
        outputTypes);
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

  public int getOutputSize() {
    return outputSize;
  }

  public List<Type> getOutputType() {
    return outputTypes;
  }

  public QualifiedName getTable() {
    return table;
  }

  public List<ColumnSchema> getTableColumns() {
    return tableColumns;
  }
}
