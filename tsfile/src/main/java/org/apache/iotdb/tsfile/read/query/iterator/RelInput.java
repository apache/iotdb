package org.apache.iotdb.tsfile.read.query.iterator;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public interface RelInput {

  class RelInputImpl implements RelInput {

    private final int index;
    private final TSDataType dataType;

    public RelInputImpl(int index, TSDataType dataType) {
      this.index = index;
      this.dataType = dataType;
    }

    @Override
    public int getInput() {
      return index;
    }

    @Override
    public TSDataType getDataType() {
      return dataType;
    }
  }

  int getInput();

  TSDataType getDataType();
}
