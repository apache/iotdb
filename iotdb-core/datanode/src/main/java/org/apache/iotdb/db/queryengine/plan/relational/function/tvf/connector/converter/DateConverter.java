package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.connector.converter;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DateConverter implements ResultSetConverter {
  @Override
  public void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException {
    java.sql.Date value = row.getDate(columnIndex);
    if (value == null || row.wasNull()) {
      properColumnBuilder.appendNull();
    } else {
      int year = value.getYear();
      int month = value.getMonth();
      int day = value.getDate();
      properColumnBuilder.writeInt((year + 1900) * 10_000 + (month + 1) * 100 + day);
    }
  }
}
