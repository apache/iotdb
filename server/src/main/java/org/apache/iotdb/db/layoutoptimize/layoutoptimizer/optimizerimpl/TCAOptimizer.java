package org.apache.iotdb.db.layoutoptimize.layoutoptimizer.optimizerimpl;

import org.apache.iotdb.db.layoutoptimize.layoutoptimizer.LayoutOptimizer;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

public class TCAOptimizer extends LayoutOptimizer {
  public TCAOptimizer(PartialPath device) {
    super(device);
  }

  @Override
  public Pair<List<String>, Long> optimize() {
    return null;
  }
}
