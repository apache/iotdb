package org.apache.iotdb.db.qp.physical.sys;

public class ShowPipeServerPlan extends ShowPlan {

  private String pipeName;

  public ShowPipeServerPlan(String pipeName) {
    super(ShowContentType.PIPESERVER);
    this.pipeName = pipeName;
  }

  public ShowPipeServerPlan() {
    super(ShowContentType.PIPESERVER);
  }

  public String getPipeName() {
    return pipeName;
  }

  public void setPipeName(String pipeName) {
    this.pipeName = pipeName;
  }
}
