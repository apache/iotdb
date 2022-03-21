package org.apache.iotdb.confignode.consensus.statemachine;

import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.service.executor.PlanExecutor;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

/** The StandAlone version StateMachine for ConfigNode, mainly used in development */
public class StandAloneStateMachine extends BaseStateMachine {

  private static final PlanExecutor executor = PlanExecutor.getInstance();

  @Override
  protected TSStatus write(PhysicalPlan plan) {
    return executor.executorNonQueryPlan(plan);
  }

  @Override
  protected DataSet read(PhysicalPlan plan) {
    return executor.executorQueryPlan(plan);
  }
}
