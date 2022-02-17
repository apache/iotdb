package org.apache.iotdb.db.qp.physical.sys;

import java.io.DataOutputStream;
import java.io.IOException;

public class ShowNowPlan extends ShowPlan {

  String ipAddress;
  String systemTime;
  String cpuLoad;
  String totalMemorySize;
  String freeMemorySize;

  public ShowNowPlan() {
    super(ShowContentType.NOW);
  }

  public ShowNowPlan(ShowContentType showContentType) {
    super(showContentType);
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    outputStream.write(PhysicalPlanType.SHOW_NOW.ordinal());
  }

  @Override
  public String toString() {
    return "ShowNowPlan{"
        + "ipAddress='"
        + ipAddress
        + '\''
        + ", systemTime='"
        + systemTime
        + '\''
        + ", cpuLoad='"
        + cpuLoad
        + '\''
        + ", totalMemorySize='"
        + totalMemorySize
        + '\''
        + ", freeMemorySize='"
        + freeMemorySize
        + '\''
        + '}';
  }
}
