package org.apache.iotdb.calcite;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

public interface IoTDBRel extends RelNode {

  void implement(Implementor implementor);

  /**
   * Calling convention for relational operations that occur in IoTDB.
   */
  Convention CONVENTION = new Convention.Impl("IOTDB", IoTDBRel.class);

  /**
   * Callback for the implementation process that converts a tree of {@link IoTDBRel} nodes into a
   * IoTDB SQL query.
   */
  class Implementor {

    final List<String> selectFields = new ArrayList<>();
    final Map<String, String> deviceToFilterMap = new LinkedHashMap<>();
    final List<String> globalPredicate = new ArrayList<>();
    int limit = 0;
    int offset = 0;

    RelOptTable table;
    IoTDBTable ioTDBTable;

    /**
     * Adds newly projected fields and .
     *
     * @param fields New fields to be projected from a query
     */
    public void addFields(List<String> fields) {
      if (selectFields != null) {
        selectFields.addAll(fields);
      }
    }

    /**
     * Adds newly restricted devices and predicates.
     *
     * @param deviceToFilterMap predicate of given device
     * @param predicates        global predicates to be applied to the query
     */
    public void add(Map<String, String> deviceToFilterMap, List<String> predicates) {
      if (this.deviceToFilterMap != null) {
        this.deviceToFilterMap.putAll(deviceToFilterMap);
      }
      if (this.globalPredicate != null) {
        this.globalPredicate.addAll(predicates);
      }
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((IoTDBRel) input).implement(this);
    }
  }
}

// End IoTDBRel.java