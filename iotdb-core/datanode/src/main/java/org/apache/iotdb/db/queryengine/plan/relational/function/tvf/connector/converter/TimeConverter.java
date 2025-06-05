package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.connector.converter;

import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class TimeConverter implements ResultSetConverter {
  @Override
  public void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException {
    java.sql.Time value = row.getTime(columnIndex);
    if (value == null || row.wasNull()) {
      properColumnBuilder.appendNull();
    } else {
      properColumnBuilder.writeLong(
          TimestampPrecisionUtils.convertToCurrPrecision(value.getTime(), TimeUnit.MILLISECONDS));
    }
  }
}
