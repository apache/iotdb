package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.BackupPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class BackupOperator extends Operator {
  String outputPath;

  public BackupOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.BACKUP;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new BackupPlan(outputPath);
  }
}
