package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPipeSinkTypePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class ShowPipeSinkTypeOperator extends ShowOperator {
  public ShowPipeSinkTypeOperator() {
    super(SQLConstant.TOK_SHOW_PIPESINKTYPE, OperatorType.SHOW_PIPESINKTYPE);
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new ShowPipeSinkTypePlan();
  }
}
