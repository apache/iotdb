package org.apache.iotdb.tsfile.file.metadata.statistics;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.PlainEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.SDTEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.ValuePoint;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

public class ValueIndex {

  int errorParam = TSFileDescriptor.getInstance().getConfig().getErrorParam();
  private DoubleArrayList values = new DoubleArrayList();
  public SDTEncoder sdtEncoder = new SDTEncoder();
  public double errorBound = 0; // =std*2 =2*std.compDeviation
  //  public IntDeltaEncoder idxEncoder = new IntDeltaEncoder();
  public PlainEncoder idxEncoder = new PlainEncoder(TSDataType.INT32, 0);
  //  public DoublePrecisionEncoderV2 valueEncoder = new DoublePrecisionEncoderV2();
  public PlainEncoder valueEncoder = new PlainEncoder(TSDataType.DOUBLE, 0);
  public PublicBAOS idxOut = new PublicBAOS();
  public PublicBAOS valueOut = new PublicBAOS();
  public IntArrayList modelPointIdx_list = new IntArrayList();
  public DoubleArrayList modelPointVal_list = new DoubleArrayList();

  public List<ValuePoint> sortedModelPoints =
      new ArrayList<>(); // sorted by value in ascending order

  // this is necessary, otherwise serialized twice by timeseriesMetadata and chunkMetadata
  // causing learn() executed more than once!!
  private boolean isLearned = false;

  public int modelPointCount = 0; // except the first and last points

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
    this.sdtEncoder.setCompDeviation(errorBound / 2 * errorParam); // equals stdDev is best
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
    boolean hasDataToFlush = false;
    for (double v : values.toArray()) {
      pos++; // starting from 1
      if (sdtEncoder.encodeDouble(pos, v)) {
        if (pos > 1) {
          // the first point value is stored as FirstValue in statistics, so here no need store the
          // first point
          // the last point won't be checked by the if SDT encode logic
          modelPointCount++;
          idxEncoder.encode((int) sdtEncoder.getTime(), idxOut);
          valueEncoder.encode(sdtEncoder.getDoubleValue(), valueOut);
          if (!hasDataToFlush) {
            hasDataToFlush = true;
          }
        }
      }
    }
    //    // add the last point except the first point
    //    if (values.size() >= 2) { // means there is last point except the first point
    //      idxEncoder.encode(pos, idxOut);
    //      valueEncoder.encode(values.getLast(), valueOut);
    //    }

    if (hasDataToFlush) {
      // otherwise no need flush, because GorillaV2 encoding will output NaN even if
      // hasDataToFlush=false
      idxEncoder.flush(idxOut); // necessary
      valueEncoder.flush(valueOut); // necessary
    }

    values = null; // raw values are not needed any more
  }
}
