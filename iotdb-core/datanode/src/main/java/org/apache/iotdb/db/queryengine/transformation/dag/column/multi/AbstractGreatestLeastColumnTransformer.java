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

package org.apache.iotdb.db.queryengine.transformation.dag.column.multi;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractGreatestLeastColumnTransformer extends MultiColumnTransformer {

  protected AbstractGreatestLeastColumnTransformer(
      Type returnType, List<ColumnTransformer> columnTransformerList) {
    super(returnType, columnTransformerList);
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      final int index = i;
      if (childrenColumns.stream().allMatch(column -> column.isNull(index))) {
        builder.appendNull();
        continue;
      }
      transform(builder, childrenColumns, index);
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount, boolean[] selection) {
    for (int i = 0; i < positionCount; i++) {
      final int index = i;
      if (selection[index] || childrenColumns.stream().allMatch(column -> column.isNull(index))) {
        builder.appendNull();
        continue;
      }
      transform(builder, childrenColumns, index);
    }
  }

  protected void transform(ColumnBuilder builder, List<Column> childrenColumns, int i) {
    TypeEnum type = columnTransformerList.get(0).getType().getTypeEnum();
    switch (type) {
      case BOOLEAN:
        transformBoolean(builder, getValues(childrenColumns, i, Column::getBoolean));
        break;
      case INT32:
      case DATE:
        transformInt(builder, getValues(childrenColumns, i, Column::getInt));
        break;
      case INT64:
      case TIMESTAMP:
        transformLong(builder, getValues(childrenColumns, i, Column::getLong));
        break;
      case FLOAT:
        transformFloat(builder, getValues(childrenColumns, i, Column::getFloat));
        break;
      case DOUBLE:
        transformDouble(builder, getValues(childrenColumns, i, Column::getDouble));
        break;
      case STRING:
      case TEXT:
        transformBinary(builder, getValues(childrenColumns, i, Column::getBinary));
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported data type: %s", returnType.getTypeEnum()));
    }
  }

  private <T> List<T> getValues(List<Column> columns, int index, ValueExtractor<T> extractor) {
    List<T> values = new ArrayList<>();
    for (Column column : columns) {
      if (!column.isNull(index)) {
        values.add(extractor.extract(column, index));
      }
    }
    return values;
  }

  @FunctionalInterface
  private interface ValueExtractor<T> {
    T extract(Column column, int index);
  }

  protected abstract void transformBoolean(ColumnBuilder builder, List<Boolean> values);

  protected abstract void transformInt(ColumnBuilder builder, List<Integer> values);

  protected abstract void transformLong(ColumnBuilder builder, List<Long> values);

  protected abstract void transformFloat(ColumnBuilder builder, List<Float> values);

  protected abstract void transformDouble(ColumnBuilder builder, List<Double> values);

  protected abstract void transformBinary(ColumnBuilder builder, List<Binary> values);

  @Override
  protected void checkType() {
    // do nothing
  }
}
