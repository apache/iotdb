package org.apache.iotdb.db.query.aggregation.impl;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.utils.ValueIterator;
import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

public class DoddsAggrResult extends AggregateResult {
  private TSDataType seriesDataType;
  private int count;
  private List<Long> outliers = new ArrayList<>(); // store the timestamps of outliers
  private int k, w, s;
  private double r;
  private Statistics statisticsInstance = new DoubleStatistics();

  // TODO: 分桶信息存到中间变量buckets中，结果信息存到结果变量outliers中

  public DoddsAggrResult(TSDataType seriesDataType) {
    super(TSDataType.VECTOR, AggregationType.DODDS);
    this.seriesDataType = seriesDataType;
    reset();
  }

  @Override
  public List<Integer> getResult() {
    // TODO: revise statistics to return the detection result

    return hasCandidateResult() ? getIntArrayValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    if (statistics.getCount() == 0) {
      return;
    }
    if (statistics instanceof DoubleStatistics) {
      count += statistics.getCount();
    } else {
      throw new StatisticsClassException("Does not support: DODDS");
    }
    //        setIntArrayValue(statisticsInstance.getBuckets());
  }

  @Override
  public void updateResultFromPageData(IBatchDataIterator batchIterator)
      throws IOException, QueryProcessException {
    updateResultFromPageData(batchIterator, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) {
    while (batchIterator.hasNext(minBound, maxBound)) {
      if (batchIterator.currentTime() >= maxBound || batchIterator.currentTime() < minBound) {
        break;
      }
      statisticsInstance.update(batchIterator.currentTime(), (double) batchIterator.currentValue());
      batchIterator.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        statisticsInstance.update(timestamps[i], (double) values[i]);
      }
    }
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, ValueIterator valueIterator) {
    int i = 0;
    while (valueIterator.hasNext() && i < length) {
      statisticsInstance.update(timestamps[i], (double) valueIterator.next());
      i++;
    }
  }

  @Override
  public boolean hasFinalResult() {
    // TODO：修改判断
    return false;
  }

  @Override
  public void merge(AggregateResult another) {
    DoddsAggrResult anotherVar = (DoddsAggrResult) another;
    if (anotherVar.getStatisticsInstance().getCount() == 0) {
      return;
    }
    //        if (another.getResult() instanceof List<?>){
    //            int i = 0;
    //            for (Object o : (List<?>)another.getResult()){
    //                mergedBuckets.set(i, mergedBuckets.get(i) + Integer.class.cast(o));
    //                i += 1;
    //            }
    //        }
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {
    this.seriesDataType = TSDataType.deserialize(buffer.get());
    //        for (int i=0; i<Statistics.bucketCnt; i++){
    //            this.mergedBuckets.set(i, buffer.getInt());
    //        }
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(seriesDataType, outputStream);
    //        for (int i=0; i<Statistics.bucketCnt; i++){
    //            ReadWriteIOUtils.write(this.mergedBuckets.get(i), outputStream);
    //        }
  }

  @Override
  public void reset() {
    super.reset();
    //        for (int i=0; i<Statistics.bucketCnt; i++){
    //            mergedBuckets.set(i, 0);
    //        }
    statisticsInstance = new DoubleStatistics();
  }

  public Statistics getStatisticsInstance() {
    return statisticsInstance;
  }

  public void setStatisticsInstance(Statistics statisticsInstance) {
    this.statisticsInstance = statisticsInstance;
  }

  public void setParameters(Map<String, String> parameters) {
    if (parameters.containsKey("k")
        && parameters.containsKey("r")
        && parameters.containsKey("w")
        && parameters.containsKey("s")) {
      this.k = Integer.parseInt(parameters.get("k"));
      this.r = Double.parseDouble(parameters.get("r"));
      this.w = Integer.parseInt(parameters.get("w"));
      this.s = Integer.parseInt(parameters.get("s"));

      System.out.println("Parameters are successfully set.");
    } else {
      System.out.println("Parameters are not obtained in AggrResult.");
    }
  }

  public double getR() {
    return r;
  }

  public int getK() {
    return k;
  }

  public int getW() {
    return w;
  }

  public int getS() {
    return s;
  }

  public void detectOutliers() {
    //        int j = (int) Math.ceil(r / Statistics.gamma);
    //        int l = (int) Math.floor(r / Statistics.gamma);
    //        int beta = Statistics.bucketCnt;
    //        int lowBound, highBound;
    //        for (int u=0; u < beta; u++){
    //            lowBound = 0;
    //            highBound = 0;
    //            for (int i = Math.max(0, u-l+1); i < Math.min(beta, u+l-1); i++) {
    //                lowBound += mergedBuckets.get(i);
    //            }
    //            if (lowBound >= k) continue;
    //
    //            for (int i = Math.max(0, u-j); i < Math.min(beta, u+j); i++){
    //                highBound += mergedBuckets.get(i);
    //            }
    //            if (j - r/Statistics.gamma >= 0.5){
    //                highBound = Math.max(highBound - mergedBuckets.get(u-j), highBound -
    // mergedBuckets.get(u+j));
    //            }
    //
    //            if (highBound < k) {continue;} //TODO: update outlier set
    //        }

  }
}
