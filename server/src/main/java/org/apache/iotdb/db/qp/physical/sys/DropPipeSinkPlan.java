package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.Collections;
import java.util.List;

public class DropPipeSinkPlan extends PhysicalPlan {
  private String pipeSinkName;

  public DropPipeSinkPlan(String pipeSinkName) {
    super(Operator.OperatorType.DROP_PIPESINK);
    this.pipeSinkName = pipeSinkName;
  }

  public String getPipeSinkName() {
    return pipeSinkName;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }
}
