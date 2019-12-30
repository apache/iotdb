package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.tsfile.read.common.Path;

public class ShowTimeSeriesPlan extends ShowPlan{
  private Path path;

  public ShowTimeSeriesPlan(ShowContentType showContentType, Path path) {
    super(showContentType);
    this.path = path;
  }

  public Path getPath() {
    return this.path;
  }
}
