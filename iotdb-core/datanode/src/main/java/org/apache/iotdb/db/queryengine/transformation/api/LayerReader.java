package org.apache.iotdb.db.queryengine.transformation.api;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import java.io.IOException;

public interface LayerReader extends YieldableReader {
  // As its name suggests,
  // This method is only used in PointReader
  boolean isConstantPointReader();

  void consumed(int count);

  void consumedAll();

  Column[] current() throws IOException;

  TSDataType[] getDataTypes();
}
