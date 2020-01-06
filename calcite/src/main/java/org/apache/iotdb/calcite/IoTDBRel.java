package org.apache.iotdb.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.*;

public interface IoTDBRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in IoTDB. */
  Convention CONVENTION = new Convention.Impl("IOTDB", IoTDBRel.class);

  /** Callback for the implementation process that converts a tree of
   * {@link IoTDBRel} nodes into a IoTDB SQL query. */
  class Implementor {
    final List<String> selectFields = new ArrayList<>();
    final List<String> whereClause = new ArrayList<>();
    int limit = 0;
    int offset = 0;

    RelOptTable table;
    IoTDBTable ioTDBTable;

    /** Adds newly projected fields and restricted predicates.
     *
     * @param fields New fields to be projected from a query
     * @param predicates New predicates to be applied to the query
     */
    public void add(List<String> fields, List<String> predicates) {
      if (selectFields != null) {
        selectFields.addAll(fields);
      }
      if (predicates != null) {
        whereClause.addAll(predicates);
      }
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((IoTDBRel) input).implement(this);
    }
  }
}

// End IoTDBRel.java