package org.apache.iotdb.db.qp.physical.sys;

public class ShowPipeSinkPlan extends ShowPlan {
  private String pipeSinkName;

  public ShowPipeSinkPlan(String pipeSinkName) {
    super(ShowContentType.PIPESINK);
    this.pipeSinkName = pipeSinkName;
  }

  public String getPipeSinkName() {
    return pipeSinkName;
  }
}
