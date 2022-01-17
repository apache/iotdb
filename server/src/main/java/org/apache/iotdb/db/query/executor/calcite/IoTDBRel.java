package org.apache.iotdb.db.query.executor.calcite;

import org.apache.calcite.interpreter.InterpretableRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public interface IoTDBRel extends RelNode {

  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in IoTDB. */
  Convention CONVENTION = new Convention.Impl("IOTDB", IoTDBRel.class);

  /** Callback for the implementation process that converts a tree of
   * {@link IoTDBRel} nodes into a MongoDB query. */
  class Implementor {
    final List<Pair<String, String>> list = new ArrayList<>();
    final RexBuilder rexBuilder;
    RelOptTable table;
    IoTDBTable ioTDBTable;
//    MongoTable mongoTable;

    public Implementor(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }
//
//    public void add(String findOp, String aggOp) {
//      list.add(Pair.of(findOp, aggOp));
//    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((IoTDBRel) input).implement(this);
    }
  }

}
