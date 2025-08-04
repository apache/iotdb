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

import java.util.List;

public class BooleanLeastColumnTransformer extends AbstractGreatestLeastColumnTransformer {
  protected BooleanLeastColumnTransformer(
      Type returnType, List<ColumnTransformer> columnTransformerList) {
    super(returnType, columnTransformerList);
  }

  @Override
  protected void transform(ColumnBuilder builder, List<Column> childrenColumns, int index) {
    boolean allNull = true;
    for (Column column : childrenColumns) {
      if (!column.isNull(index)) {
        allNull = false;
        if (!column.getBoolean(index)) {
          returnType.writeBoolean(builder, false);
          return;
        }
      }
    }
    if (allNull) {
      builder.appendNull();
    } else {
      returnType.writeBoolean(builder, true);
    }
  }
}
