package org.apache.iotdb.db.utils.quantiles;

import org.apache.datasketches.kll.KllDoublesSketch;

import java.util.ArrayList;
import java.util.List;

public class KLLDoublesForMedian { // doubles in fact..
  public KllDoublesSketch sketch;
  final int KllK = 45000;
  //  final double KllEpsilon;
  public KLLDoublesForMedian() {

    sketch = KllDoublesSketch.newHeapInstance(KllK);
    //    KllEpsilon = sketch.getNormalizedRankError(false)*1.4;
  }

  public void add(final double value) {
    sketch.update(value);
  }

  public List<Double> findResultRange(long K1, long K2) {
    List<Double> result = new ArrayList<>(4);
    long n = sketch.getN();
    result.add((double) sketch.getQuantileLowerBound(1.0d * K1 / n));
    result.add((double) sketch.getQuantileUpperBound(1.0d * K2 / n));
    return result;
  }

  public boolean isExactResult() {
    return sketch.getN() <= KllK;
  }

  public void reset() {
    sketch = KllDoublesSketch.newHeapInstance(KllK);
  }
}
