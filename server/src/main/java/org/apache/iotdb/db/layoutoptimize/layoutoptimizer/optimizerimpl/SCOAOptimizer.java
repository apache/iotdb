package org.apache.iotdb.db.layoutoptimize.layoutoptimizer.optimizerimpl;

import org.apache.iotdb.db.layoutoptimize.layoutoptimizer.LayoutOptimizer;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

public class SCOAOptimizer extends LayoutOptimizer {
  public SCOAOptimizer(String deviceId) {
    super(deviceId);
  }

  @Override
  public Pair<List<String>, Long> optimize() {
    return null;
  }
}
