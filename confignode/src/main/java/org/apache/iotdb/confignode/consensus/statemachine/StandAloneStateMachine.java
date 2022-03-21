package org.apache.iotdb.confignode.consensus.statemachine;

import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.service.executor.PlanExecutor;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The StandAlone version StateMachine for ConfigNode, mainly used in development */
public class StandAloneStateMachine extends BaseStateMachine {

  private static final Logger LOGGER = LoggerFactory.getLogger(StandAloneStateMachine.class);

  private static final PlanExecutor executor = PlanExecutor.getInstance();

  @Override
  protected TSStatus write(PhysicalPlan plan) {
    TSStatus result;
    try {
      result = executor.executorNonQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException e) {
      LOGGER.error(e.getMessage());
      result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return result;
  }

  @Override
  protected DataSet read(PhysicalPlan plan) {
    DataSet result;
    try {
      result = executor.executorQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException e) {
      LOGGER.error(e.getMessage());
      result = null;
    }
    return result;
  }
}
