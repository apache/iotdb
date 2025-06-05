package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.connector.converter;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetConverter {
  void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException;
}
