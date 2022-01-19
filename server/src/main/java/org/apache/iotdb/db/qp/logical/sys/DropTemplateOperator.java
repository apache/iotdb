package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class DropTemplateOperator extends Operator {

  private String templateName;

  public DropTemplateOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.DROP_TEMPLATE;
  }

  public String getTemplateName() {
    return templateName;
  }

  public void setTemplateName(String templateName) {
    this.templateName = templateName;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new DropTemplatePlan(templateName);
  }
}
