package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

public class BlobLengthColumnTransformer extends UnaryColumnTransformer {


  public BlobLengthColumnTransformer(Type returnType, ColumnTransformer childColumnTransformer) {
    super(returnType, childColumnTransformer);
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    doTransform(column, columnBuilder, null);
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {

    int positionCount = column.getPositionCount();
    for (int i = 0; i < positionCount; i++) {
      if ((selection != null && !selection[i]) || column.isNull(i)) {
        columnBuilder.appendNull();
        continue;
      }

      Binary value = column.getBinary(i);
      int length = value.getValues().length;
      columnBuilder.writeLong(length);
    }
  }
}
