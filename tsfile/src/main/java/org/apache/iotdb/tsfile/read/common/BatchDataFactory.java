package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class BatchDataFactory {

  public static BatchData createBatchData(TSDataType dataType, boolean ascending) {
    if (ascending) {
      return new BatchData(dataType);
    }
    return new DescBatchData(dataType);
  }

  public static BatchData createBatchData(TSDataType dataType) {
    return new BatchData(dataType);
  }

}
