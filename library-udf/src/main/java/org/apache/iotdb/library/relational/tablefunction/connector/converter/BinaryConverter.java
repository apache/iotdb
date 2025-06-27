package org.apache.iotdb.library.relational.tablefunction.connector.converter;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.Binary;

import java.sql.ResultSet;
import java.sql.SQLException;

public class BinaryConverter implements ResultSetConverter {
  @Override
  public void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException {
    byte[] value = row.getBytes(columnIndex);
    if (value == null || row.wasNull()) {
      properColumnBuilder.appendNull();
    } else {
      properColumnBuilder.writeBinary(new Binary(value));
    }
  }
}
