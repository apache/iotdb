package org.apache.iotdb.db.index.common;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.TVList;

public class DistSeries {

  public double dist;
  public TVList tvList;
  public PartialPath partialPath;

  public DistSeries(double dist, TVList tvList, PartialPath partialPath) {
    this.dist = dist;
    this.tvList = tvList;
    this.partialPath = partialPath;
  }

  public String toString() {
    return "(" + partialPath + "," + dist + ":" + tvList + ")";
  }
}
