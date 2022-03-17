package org.apache.iotdb.db.query.aggregation.impl;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.utils.ValueIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class CurrentValueAggrResult extends AggregateResult {

  public CurrentValueAggrResult(TSDataType seriesDataType) {
    super(TSDataType.TEXT, AggregationType.CURRENT);
    reset();
  }

  @Override
  public Object getResult() {
    return hasCandidateResult() ? getValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) throws QueryProcessException {
    Comparable<Object> currentValue = (Comparable<Object>) statistics.getCurrentValue();
    setValue(currentValue);
  }

  @Override
  public void updateResultFromPageData(IBatchDataIterator batchIterator)
      throws IOException, QueryProcessException {}

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) throws IOException {}

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {}

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, ValueIterator valueIterator) {}

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void merge(AggregateResult another) {}

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {}

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {}
}
