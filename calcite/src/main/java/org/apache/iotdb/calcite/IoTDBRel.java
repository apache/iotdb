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
    final List<String> fromClause = new ArrayList<>();
    final List<String> whereClause = new ArrayList<>();
    int limit = 0;
    int offset = 0;

    RelOptTable table;
    IoTDBTable ioTDBTable;

    /** Adds newly projected fields and .
     *
     * @param fields New fields to be projected from a query
     */
    public void addFields(List<String> fields) {
      if (selectFields != null) {
        selectFields.addAll(fields);
      }
    }

    /** Adds newly restricted devices and predicates.
     *
     * @param devices New devices to be queried in from clause
     * @param predicates New predicates to be applied to the query
     */
    public void add(List<String> devices, List<String> predicates){
      if(fromClause != null){
        fromClause.addAll(devices);
      }
      if(predicates != null){
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