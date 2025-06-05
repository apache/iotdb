package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.connector.converter;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;

import java.sql.ResultSet;
import java.sql.SQLException;

public class StringConverter implements ResultSetConverter {

  @Override
  public void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException {
    String value = row.getString(columnIndex);
    if (row.wasNull()) {
      properColumnBuilder.appendNull();
    } else {
      properColumnBuilder.writeBinary(new Binary(value, TSFileConfig.STRING_CHARSET));
    }
  }
}
