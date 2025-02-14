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
import org.apache.tsfile.utils.Binary;

import java.util.List;

public class BinaryLeastColumnTransformer extends AbstractGreatestLeastColumnTransformer {
  protected BinaryLeastColumnTransformer(
      Type returnType, List<ColumnTransformer> columnTransformerList) {
    super(returnType, columnTransformerList);
  }

  @Override
  protected void transform(ColumnBuilder builder, List<Column> childrenColumns, int index) {
    Column firstColumn = childrenColumns.get(0);
    Binary minValue = firstColumn.getBinary(index);
    boolean allNull = firstColumn.isNull(index);
    for (int i = 1; i < childrenColumns.size(); i++) {
      Column column = childrenColumns.get(i);
      if (!column.isNull(index)) {
        Binary value = column.getBinary(index);
        if (allNull || value.compareTo(minValue) < 0) {
          allNull = false;
          minValue = value;
        }
      }
    }
    if (allNull) {
      builder.appendNull();
    } else {
      returnType.writeBinary(builder, minValue);
    }
  }
}
