package org.apache.iotdb.library.relational.tablefunction.connector.converter;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Int32Converter implements ResultSetConverter {
  @Override
  public void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException {
    int value = row.getInt(columnIndex);
    if (row.wasNull()) {
      properColumnBuilder.appendNull();
    } else {
      properColumnBuilder.writeInt(value);
    }
  }
}
