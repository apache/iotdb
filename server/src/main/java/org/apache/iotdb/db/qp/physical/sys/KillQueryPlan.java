package org.apache.iotdb.db.qp.physical.sys;

import java.util.List;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public class KillQueryPlan extends PhysicalPlan {

  private int queryId = -1;

  public KillQueryPlan(int queryId) {
    super(false, OperatorType.KILL);
    this.queryId = queryId;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  public int getQueryId() {
    return queryId;
  }
}
