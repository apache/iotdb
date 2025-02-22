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
      transform(builder, childrenColumns, i);
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount, boolean[] selection) {
    for (int i = 0; i < positionCount; i++) {
      if (selection[i]) {
        transform(builder, childrenColumns, i);
      } else {
        builder.appendNull();
      }
    }
  }

  protected abstract void transform(ColumnBuilder builder, List<Column> childrenColumns, int index);

  public static ColumnTransformer getGreatestColumnTransformer(
      Type type, List<ColumnTransformer> columnTransformers) {
    TypeEnum typeEnum = type.getTypeEnum();
    switch (typeEnum) {
      case BOOLEAN:
        return new BooleanGreatestColumnTransformer(type, columnTransformers);
      case INT32:
      case DATE:
        return new Int32GreatestColumnTransformer(type, columnTransformers);
      case INT64:
      case TIMESTAMP:
        return new Int64GreatestColumnTransformer(type, columnTransformers);
      case FLOAT:
        return new FloatGreatestColumnTransformer(type, columnTransformers);
      case DOUBLE:
        return new DoubleGreatestColumnTransformer(type, columnTransformers);
      case STRING:
      case TEXT:
        return new BinaryGreatestColumnTransformer(type, columnTransformers);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported data type: %s", typeEnum));
    }
  }

  public static ColumnTransformer getLeastColumnTransformer(
      Type type, List<ColumnTransformer> columnTransformers) {
    TypeEnum typeEnum = type.getTypeEnum();
    switch (typeEnum) {
      case BOOLEAN:
        return new BooleanLeastColumnTransformer(type, columnTransformers);
      case INT32:
      case DATE:
        return new Int32LeastColumnTransformer(type, columnTransformers);
      case INT64:
      case TIMESTAMP:
        return new Int64LeastColumnTransformer(type, columnTransformers);
      case FLOAT:
        return new FloatLeastColumnTransformer(type, columnTransformers);
      case DOUBLE:
        return new DoubleLeastColumnTransformer(type, columnTransformers);
      case STRING:
      case TEXT:
        return new BinaryLeastColumnTransformer(type, columnTransformers);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported data type: %s", typeEnum));
    }
  }

  @Override
  protected void checkType() {
    // do nothing
  }
}
