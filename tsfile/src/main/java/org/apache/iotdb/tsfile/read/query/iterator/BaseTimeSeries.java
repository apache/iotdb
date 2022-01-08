package org.apache.iotdb.tsfile.read.query.iterator;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public abstract class BaseTimeSeries implements TimeSeries {

  private final TSDataType[] dataTypes;

  public BaseTimeSeries(List<TSDataType> dataTypes) {
    this(dataTypes.toArray(new TSDataType[0]));
  }

  public BaseTimeSeries(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
  }

  @Override
  public TSDataType[] getSpecification() {
    return this.dataTypes;
  }
}
