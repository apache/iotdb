package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class DescBatchData extends BatchData {

  public DescBatchData(TSDataType dataType) {
    super(dataType);
  }

  @Override
  public boolean hasCurrent() {
    return super.readCurListIndex >= 0 && super.readCurArrayIndex >= 0;
  }

  @Override
  public void next() {
    super.readCurArrayIndex--;
    if (super.readCurArrayIndex == -1) {
      super.readCurArrayIndex = capacity;
      super.readCurListIndex--;
    }
  }

  @Override
  public void resetBatchData() {
    super.readCurArrayIndex = writeCurArrayIndex - 1;
    super.readCurListIndex = writeCurListIndex;
  }

  @Override
  public BatchData flip() {
    super.readCurArrayIndex = writeCurArrayIndex - 1;
    super.readCurListIndex = writeCurListIndex;
    return this;
  }
}
