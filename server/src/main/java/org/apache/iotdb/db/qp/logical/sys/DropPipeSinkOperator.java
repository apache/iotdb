package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.DropPipeSinkPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class DropPipeSinkOperator extends Operator {
  private String pipeSinkName;

  public DropPipeSinkOperator(String pipeSinkName) {
    super(SQLConstant.TOK_DROP_PIPESINK);
    this.pipeSinkName = pipeSinkName;
    this.operatorType = OperatorType.DROP_PIPESINK;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new DropPipeSinkPlan(pipeSinkName);
  }
}
