package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.Collections;
import java.util.List;

public class ShowPipeSinkPlan extends PhysicalPlan {
  private String pipeSinkName;

  public ShowPipeSinkPlan(String pipeSinkName) {
    super(false, Operator.OperatorType.SHOW_PIPESINK);
    this.pipeSinkName = pipeSinkName;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }
}
