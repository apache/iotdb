package org.apache.iotdb.db.layoutoptimize.layoutoptimizer.optimizerimpl;

import org.apache.iotdb.db.layoutoptimize.layoutoptimizer.LayoutOptimizer;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

public class TCAOptimizer extends LayoutOptimizer {
  public TCAOptimizer(String deviceId) {
    super(deviceId);
  }

  @Override
  public Pair<List<String>, Long> optimize() {
    return null;
  }
}
