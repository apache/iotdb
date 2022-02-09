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

public class ValidityAggrResult extends AggregateResult {
  private TSDataType seriesDataType;
  private double validity = 0.0;
  private double sum = 0.0;
  private long cnt = 0;
  private Statistics statisticsInstance = new DoubleStatistics();

  public ValidityAggrResult(TSDataType seriesDataType) {
    super(TSDataType.DOUBLE, AggregationType.VALIDITY);
    this.seriesDataType = seriesDataType;
    reset();
  }

  @Override
  public Double getResult() {
    if (statisticsInstance.getCount() > 0) {
      setDoubleValue(statisticsInstance.getValidity());
    }
    return hasCandidateResult() ? getDoubleValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    double speedBetween = 0;
    // skip empty statistics
    if (statistics.getCount() == 0) {
      return;
    }
    // update intermediate results from statistics
    // must be sure no overlap between two statistics
    if (statistics instanceof DoubleStatistics) {
      statisticsInstance.mergeStatistics(statistics);
    } else {
      throw new StatisticsClassException("Does not support: validity");
    }
    setDoubleValue(statisticsInstance.getValidity());
  }

  @Override
  public void updateResultFromPageData(IBatchDataIterator batchIterator)
      throws IOException, QueryProcessException {
    updateResultFromPageData(batchIterator, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  public void updateDPAndReverseDP() {
    if (statisticsInstance.getTimeWindow().size() == 0) {
      return;
    }
    statisticsInstance.updateDP();
    statisticsInstance.updateReverseDP();
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) throws IOException {
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
  // TODO：修改判断
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void merge(AggregateResult another) {
    // TODO: merge need be updated
    ValidityAggrResult anotherVar = (ValidityAggrResult) another;
    // skip empty results
    if (anotherVar.getStatisticsInstance().getCount() == 0) {
      return;
    }
    validity +=
        (anotherVar.getResult() * anotherVar.getStatisticsInstance().getCount()
                + validity * statisticsInstance.getCount())
            / (anotherVar.getStatisticsInstance().getCount() + statisticsInstance.getCount());
    setDoubleValue(validity);
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {
    this.seriesDataType = TSDataType.deserialize(buffer.get());
    this.validity = buffer.getDouble();
    this.sum = buffer.getDouble();
    this.cnt = buffer.getLong();
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(seriesDataType, outputStream);
    ReadWriteIOUtils.write(validity, outputStream);
    ReadWriteIOUtils.write(sum, outputStream);
    ReadWriteIOUtils.write(cnt, outputStream);
  }

  @Override
  public void reset() {
    super.reset();
    validity = 0.0;
    sum = 0.0;
    cnt = 0L;
  }

  public double getValidity() {
    return validity;
  }

  public double getSum() {
    return sum;
  }

  public long getCnt() {
    return cnt;
  }

  public Statistics getStatisticsInstance() {
    return statisticsInstance;
  }

  public boolean checkMergeable(Statistics statistics) {
    //    if (true) {
    //      return true;
    //    }
    return statisticsInstance.checkMergeable(statistics);
  }
}
