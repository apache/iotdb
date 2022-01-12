package org.apache.iotdb.db.qp.physical.sys;

public class ShowPipePlan extends ShowPlan {

  private String pipeName;

  public ShowPipePlan(String pipeName) {
    super(ShowContentType.PIPE);
    this.pipeName = pipeName;
  }

  public ShowPipePlan() {
    super(ShowContentType.PIPE);
  }

  public String getPipeName() {
    return pipeName;
  }

  public void setPipeName(String pipeName) {
    this.pipeName = pipeName;
  }
}
