package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.connector.converter;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.Binary;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BlobConverter implements ResultSetConverter {
  @Override
  public void append(ResultSet row, int columnIndex, ColumnBuilder properColumnBuilder)
      throws SQLException {
    Blob blob = row.getBlob(columnIndex);
    if (blob == null || row.wasNull()) {
      properColumnBuilder.appendNull();
    } else {
      properColumnBuilder.writeBinary(new Binary(blob.getBytes(1, (int) blob.length())));
    }
  }
}
