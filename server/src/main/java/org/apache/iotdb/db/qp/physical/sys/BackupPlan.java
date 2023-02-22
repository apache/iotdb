package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.List;

public class BackupPlan extends PhysicalPlan {
  private String outputPath;

  public BackupPlan(String outputPath) {
    super(Operator.OperatorType.BACKUP);
    this.outputPath = outputPath;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return null;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  public String getOutputPath() {
    return outputPath;
  }
}
