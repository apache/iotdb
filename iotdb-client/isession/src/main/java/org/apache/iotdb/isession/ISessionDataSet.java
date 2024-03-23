package org.apache.iotdb.isession;

import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.List;

public interface ISessionDataSet extends AutoCloseable {

  List<String> getColumnNames();

  List<String> getColumnTypes();

  boolean hasNext();

  RowRecord next();
}
