package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.List;

public class SetDeviceTemplatePlan extends PhysicalPlan {
  String templateName;
  String prefixPath;

  public SetDeviceTemplatePlan(String templateName, String prefixPath) {
    super(false, OperatorType.SET_DEVICE_TEMPLATE);
    this.templateName = templateName;
    this.prefixPath = prefixPath;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }
}
