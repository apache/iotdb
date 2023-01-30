package org.apache.iotdb.db.query.aggregation.impl;

import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.utils.ValueIterator;
import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Predicate;

public class ArAggrResult extends AggregateResult {
  private TSDataType seriesDataType;
  private long cnt = 0;
  private int p;
  private String statisticLevel;
  private double[] coefficients;
  private Statistics statisticsInstance = new DoubleStatistics();

  public ArAggrResult(TSDataType seriesDataType) {
    super(TSDataType.DOUBLE, AggregationType.AR);
    this.seriesDataType = seriesDataType;
    reset();

    cnt = 0;
  }

  public void setParameters(Map<String, String> parameters) {
    if (parameters.containsKey("p")) this.p = Integer.parseInt(parameters.get("p"));
    else this.p = 1;
    if (parameters.containsKey("level")) this.statisticLevel = parameters.get("level");
    else this.statisticLevel = "page";
    System.out.println(
        "Parameters [p=" + p + ", statistic_level=" + statisticLevel + "] is successfully set.");
  }

  public void setCoefficients(double[] coefficients) {
    this.coefficients = coefficients;
  }

  public double[] getCoefficients() {
    return this.coefficients;
  }

  public int getParameterP() {
    return this.p;
  }

  public String getStatisticLevel() {
    return this.statisticLevel;
  }

  @Override
  protected boolean hasCandidateResult() {
    return cnt > 0;
  }

  @Override
  public Double getResult() {
    if (this.coefficients != null) setDoubleValue(this.coefficients[0]);
    return hasCandidateResult() ? getDoubleValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    if (statistics.getCount() == 0) {
      return;
    }
    if (statistics instanceof DoubleStatistics) {
      statisticsInstance.mergeStatistics(statistics);
    } else {
      throw new StatisticsClassException("Does not support");
    }
  }

  @Override
  public void updateResultFromPageData(IBatchDataIterator batchIterator) {
    updateResultFromPageData(batchIterator, time -> false);
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, Predicate<Long> boundPredicate) {
    while (batchIterator.hasNext(boundPredicate)
        && !boundPredicate.test(batchIterator.currentTime())) {
      statisticsInstance.update(batchIterator.currentTime(), (double) batchIterator.currentValue());
      batchIterator.next();
    }
  }

  public void updateTimeAndValueWindowFromPageData(IBatchDataIterator batchIterator) {
    Predicate<Long> boundPredicate = time -> false;
    while (batchIterator.hasNext(boundPredicate)
        && !boundPredicate.test(batchIterator.currentTime())) {
      statisticsInstance.updateTimeAndValueWindow(
          batchIterator.currentTime(), (double) batchIterator.currentValue());
      batchIterator.next();
    }
  }

  public void updateTimeAndValueWindowFromBatchData(BatchData batchData) {
    while (batchData.hasCurrent()) {
      statisticsInstance.updateTimeAndValueWindow(
          batchData.currentTime(), (double) batchData.currentValue());
      batchData.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    return;
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
    return false;
  }

  @Override
  public void merge(AggregateResult another) {
    // TODO: merge two ArAggrResult

  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {
    this.seriesDataType = TSDataType.deserialize(buffer.get());
    this.cnt = buffer.getLong();
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(seriesDataType, outputStream);
    ReadWriteIOUtils.write(cnt, outputStream);
  }

  @Override
  public void reset() {
    super.reset();
    this.statisticsInstance = null;
    cnt = 0;
  }

  public void setStatisticsInstance(Statistics statistics) {
    this.statisticsInstance = statistics;
  }

  public Statistics getStatisticsInstance() {
    return statisticsInstance;
  }
}
