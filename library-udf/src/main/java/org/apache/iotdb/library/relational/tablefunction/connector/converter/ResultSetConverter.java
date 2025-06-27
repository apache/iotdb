package org.apache.iotdb.library.relational.tablefunction.connector.converter;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetConverter {
  void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException;
}
