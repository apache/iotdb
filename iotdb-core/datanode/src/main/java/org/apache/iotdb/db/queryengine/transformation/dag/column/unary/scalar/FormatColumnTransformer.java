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

import org.apache.iotdb.db.exception.sql.SemanticException;
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
import java.util.IllegalFormatConversionException;
import java.util.List;
import java.util.MissingFormatArgumentException;

import static java.lang.String.format;
import static org.apache.iotdb.pipe.api.type.Binary.stringToBytes;

public class FormatColumnTransformer extends MultiColumnTransformer {

  private final ZoneId zoneId;

  public FormatColumnTransformer(
      Type returnType, List<ColumnTransformer> columnTransformerList, ZoneId zoneId) {
    super(returnType, columnTransformerList);
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
        transform(childrenColumns, builder, i);
      } else {
        builder.appendNull();
      }
    }
  }

  private void transform(List<Column> childrenColumns, ColumnBuilder builder, int i) {
    List<Column> valueColumns = childrenColumns.subList(1, childrenColumns.size());
    Object[] values = new Object[valueColumns.size()];
    String pattern = String.valueOf(childrenColumns.get(0).getBinary(i));
    for (int j = 0; j < valueColumns.size(); j++) {
      Column column = valueColumns.get(j);
      TypeEnum type = columnTransformerList.get(j + 1).getType().getTypeEnum();
      if (column.isNull(i)) {
        values[j] = null;
      } else {
        values[j] = valueConverter(type, column, i);
      }
    }
    try {
      String formatted = format(pattern, values);
      returnType.writeBinary(builder, new Binary(stringToBytes(formatted)));
    } catch (IllegalFormatConversionException | MissingFormatArgumentException e) {
      String message = e.toString().replaceFirst("^java\\.util\\.(\\w+)Exception", "$1");
      throw new SemanticException(
          String.format("Invalid format string: %s (%s)", pattern, message));
    }
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
        return DateUtils.parseIntToLocalDate(column.getInt(i));
      case TIMESTAMP:
        return DateTimeUtils.convertToZonedDateTime(column.getLong(i), zoneId);
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
