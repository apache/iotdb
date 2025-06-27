package org.apache.iotdb.library.relational.tablefunction.connector.converter;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Int64Converter implements ResultSetConverter {
  @Override
  public void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException {
    long value = row.getLong(columnIndex);
    if (row.wasNull()) {
      properColumnBuilder.appendNull();
    } else {
      properColumnBuilder.writeLong(value);
    }
  }
}
