package org.apache.iotdb.db.qp.physical.sys;

public class ShowPipeServerPlan extends ShowPlan {
  public ShowPipeServerPlan() {
    super(ShowContentType.PIPESERVER);
  }
}
