package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPipeSinkPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class ShowPipeSinkOperator extends Operator {
  private String pipeSinkName;

  public ShowPipeSinkOperator() {
    super(SQLConstant.TOK_SHOW_PIPESINK);
    pipeSinkName = "";
    this.operatorType = OperatorType.SHOW_PIPESINK;
  }

  public void setPipeSinkName(String pipeSinkName) {
    this.pipeSinkName = pipeSinkName;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new ShowPipeSinkPlan(pipeSinkName);
  }
}
