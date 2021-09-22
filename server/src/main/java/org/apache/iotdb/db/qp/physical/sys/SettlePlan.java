package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.List;

public class SettlePlan extends PhysicalPlan {
  PartialPath sgPath;

  public SettlePlan(PartialPath sgPath) {
    super(false, OperatorType.SETTLE);
    this.sgPath = sgPath;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  public PartialPath getSgPath() {
    return sgPath;
  }
}
