package org.apache.iotdb.library.relational.tablefunction.connector.converter;

import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class TimestampConverter implements ResultSetConverter {
  @Override
  public void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException {
    java.sql.Timestamp value = row.getTimestamp(columnIndex);
    if (row.wasNull()) {
      properColumnBuilder.appendNull();
    } else {
      properColumnBuilder.writeLong(
          TimestampPrecisionUtils.convertToCurrPrecision(value.getTime(), TimeUnit.MILLISECONDS));
    }
  }
}
