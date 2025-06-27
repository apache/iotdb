package org.apache.iotdb.library.relational.tablefunction.connector.converter;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

public class BooleanConverter implements ResultSetConverter {
  @Override
  public void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException {
    boolean value = row.getBoolean(columnIndex);
    if (row.wasNull()) {
      properColumnBuilder.appendNull();
    } else {
      properColumnBuilder.writeBoolean(value);
    }
  }
}
