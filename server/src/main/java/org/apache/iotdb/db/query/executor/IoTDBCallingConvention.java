package org.apache.iotdb.db.query.executor;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;

public class IoTDBCallingConvention extends Convention.Impl {

  public IoTDBCallingConvention(String name, Class<? extends RelNode> relClass) {
    super(name, relClass);
  }

  @Override public void register(RelOptPlanner planner) {
//    for (RelOptRule rule : IoTDBRules.rules(this)) {
//      planner.addRule(rule);
//    }
//    planner.addRule(CoreRules.FILTER_SET_OP_TRANSPOSE);
//    planner.addRule(CoreRules.PROJECT_REMOVE);
  }

}
