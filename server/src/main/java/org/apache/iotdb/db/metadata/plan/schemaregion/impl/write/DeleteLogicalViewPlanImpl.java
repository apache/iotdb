package org.apache.iotdb.db.metadata.plan.schemaregion.impl.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.view.IDeleteLogicalViewPlan;

public class DeleteLogicalViewPlanImpl implements IDeleteLogicalViewPlan {

  private PartialPath path;

  DeleteLogicalViewPlanImpl() {}

  DeleteLogicalViewPlanImpl(PartialPath path) {
    this.path = path;
  }

  @Override
  public PartialPath getPath() {
    return path;
  }

  @Override
  public void setPath(PartialPath path) {
    this.path = path;
  }
}
