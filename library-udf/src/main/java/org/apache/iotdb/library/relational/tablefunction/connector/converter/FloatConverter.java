package org.apache.iotdb.library.relational.tablefunction.connector.converter;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

public class FloatConverter implements ResultSetConverter {
  @Override
  public void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException {
    float value = row.getFloat(columnIndex);
    if (row.wasNull()) {
      properColumnBuilder.appendNull();
    } else {
      properColumnBuilder.writeFloat(value);
    }
  }
}
