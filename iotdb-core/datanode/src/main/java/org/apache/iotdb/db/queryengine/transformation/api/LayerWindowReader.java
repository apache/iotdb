package org.apache.iotdb.db.queryengine.transformation.api;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import java.io.IOException;

public interface LayerWindowReader {

  boolean next() throws IOException, QueryProcessException;

  void readyForNext() throws IOException, QueryProcessException;

  TSDataType[] getDataTypes();

  Column[] currentWindow();
}
