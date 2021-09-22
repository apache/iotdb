package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.SettlePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class SettleOperator extends Operator {
  PartialPath storageGroupPath;

  public SettleOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.SETTLE;
  }

  public PartialPath getStorageGroupPath() {
    return storageGroupPath;
  }

  public void setStorageGroupPath(PartialPath storageGroupPath) {
    this.storageGroupPath = storageGroupPath;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new SettlePlan(getStorageGroupPath());
  }
}
