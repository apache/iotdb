package org.apache.iotdb.library.relational.tablefunction.connector.converter;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TimeConverter implements ResultSetConverter {
  @Override
  public void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException {
    java.sql.Time value = row.getTime(columnIndex);
    if (value == null || row.wasNull()) {
      properColumnBuilder.appendNull();
    } else {
      properColumnBuilder.writeLong(value.getTime());
    }
  }
}
