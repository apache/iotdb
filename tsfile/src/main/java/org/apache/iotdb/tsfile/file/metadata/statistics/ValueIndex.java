package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder.IntDeltaEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionEncoderV2;
import org.apache.iotdb.tsfile.encoding.encoder.SDTEncoder;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

public class ValueIndex {

  private DoubleArrayList values = new DoubleArrayList();
  public SDTEncoder sdtEncoder = new SDTEncoder();
  public double errorBound = 0; // =std*2 =2*std.compDeviation
  public IntDeltaEncoder idxEncoder = new IntDeltaEncoder();
  public DoublePrecisionEncoderV2 valueEncoder = new DoublePrecisionEncoderV2();
  public PublicBAOS idxOut = new PublicBAOS();
  public PublicBAOS valueOut = new PublicBAOS();
  public int lastIdx = 0;
  public int lastIntValue = 0;
  public long lastLongValue = 0;
  public float lastFloatValue = 0;
  public double lastDoubleValue = 0;
  public IntArrayList modelPointIdx_list = new IntArrayList();
  public DoubleArrayList modelPointVal_list = new DoubleArrayList();

  // this is necessary, otherwise serialized twice by timeseriesMetadata and chunkMetadata
  // causing learn() executed more than once!!
  private boolean isLearned = false;

  private double stdDev = 0; // standard deviation of intervals
  private long count = 0;
  private double sumX2 = 0.0;
  private double sumX1 = 0.0;

  public void insert(int value) {
    values.add((double) value);
    count++;
    sumX1 += (double) value;
    sumX2 += (double) value * (double) value;
  }

  public void insert(long value) {
    values.add((double) value);
    count++;
    sumX1 += (double) value;
    sumX2 += (double) value * (double) value;
  }

  public void insert(float value) {
    values.add((double) value);
    count++;
    sumX1 += (double) value;
    sumX2 += (double) value * (double) value;
  }

  public void insert(double value) {
    values.add(value);
    count++;
    sumX1 += value;
    sumX2 += value * value;
  }

  public double getStdDev() { // sample standard deviation
    double std = Math.sqrt(this.sumX2 / this.count - Math.pow(this.sumX1 / this.count, 2));
    return Math.sqrt(Math.pow(std, 2) * this.count / (this.count - 1));
  }

  private void initForLearn() {
    this.stdDev = getStdDev();
    this.errorBound = 2 * stdDev;
    this.sdtEncoder.setCompDeviation(errorBound / 2); // equals stdDev is best
  }

  public void learn() {
    if (isLearned) {
      // this is necessary, otherwise serialized twice by timeseriesMetadata and chunkMetadata
      // causing learn() executed more than once!!
      return;
    }
    isLearned = true;
    initForLearn(); // set self-adapting CompDeviation for sdtEncoder
    int pos = 0;
    for (double v : values.toArray()) {
      pos++; // starting from 1
      if (sdtEncoder.encodeDouble(pos, v)) {
        idxEncoder.encode((int) sdtEncoder.getTime(), idxOut);
        valueEncoder.encode(sdtEncoder.getDoubleValue(), valueOut);
      }
    }
    // add the last point except the first point
    if (values.size() >= 2) { // means there is last point except the first point
      idxEncoder.encode(pos, idxOut);
      valueEncoder.encode(values.getLast(), valueOut);
    }
    idxEncoder.flush(idxOut);
    valueEncoder.flush(valueOut);
  }
}
