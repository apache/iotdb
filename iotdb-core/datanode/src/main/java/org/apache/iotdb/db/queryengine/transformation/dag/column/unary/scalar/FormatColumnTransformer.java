/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.MultiColumnTransformer;
import org.apache.iotdb.db.utils.DateTimeUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;

import java.time.ZoneId;
import java.util.List;

import static java.lang.String.format;
import static org.apache.iotdb.pipe.api.type.Binary.stringToBytes;

public class FormatColumnTransformer extends MultiColumnTransformer {

  private final String pattern;
  private final ZoneId zoneId;

  public FormatColumnTransformer(
      Type returnType,
      String pattern,
      List<ColumnTransformer> columnTransformerList,
      ZoneId zoneId) {
    super(returnType, columnTransformerList);
    this.pattern = pattern;
    this.zoneId = zoneId;
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      transform(childrenColumns, builder, i);
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount, boolean[] selection) {
    for (int i = 0; i < positionCount; i++) {
      if (selection[i]) {
        builder.appendNull();
      } else {
        transform(childrenColumns, builder, i);
      }
    }
  }

  private void transform(List<Column> childrenColumns, ColumnBuilder builder, int i) {
    Object[] values = new Object[childrenColumns.size()];
    for (int j = 0; j < childrenColumns.size(); j++) {
      Column column = childrenColumns.get(j);
      TypeEnum type = columnTransformerList.get(j).getType().getTypeEnum();
      if (column.isNull(i)) {
        values[j] = null;
      } else {
        values[j] = valueConverter(type, column, i);
      }
    }
    String formatted = format(pattern, values);
    returnType.writeBinary(builder, new Binary(stringToBytes(formatted)));
  }

  private Object valueConverter(TypeEnum type, Column column, int i) {
    switch (type) {
      case UNKNOWN:
        return null;
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case TEXT:
      case STRING:
      case BLOB:
        return column.getObject(i);
      case DATE:
        long timestamp =
            DateTimeUtils.correctPrecision(DateUtils.parseIntToTimestamp(column.getInt(i), zoneId));
        return DateTimeUtils.convertToLocalDate(timestamp, zoneId);
      case TIMESTAMP:
        return DateTimeUtils.convertToLocalDate(column.getLong(i), zoneId);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported source dataType: %s", type));
    }
  }

  @Override
  protected void checkType() {
    // do nothing because the type is checked in tableMetaDataImpl
  }
}
