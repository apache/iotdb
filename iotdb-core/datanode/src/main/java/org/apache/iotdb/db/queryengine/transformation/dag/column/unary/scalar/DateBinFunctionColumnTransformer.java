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
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

public class DateBinFunctionColumnTransformer extends UnaryColumnTransformer {

  private final long monthDuration;
  private final long nonMonthDuration;
  private final long origin;

  public DateBinFunctionColumnTransformer(
      Type returnType,
      long monthDuration,
      long nonMonthDuration,
      ColumnTransformer childColumnTransformer,
      long origin) {
    super(returnType, childColumnTransformer);
    this.monthDuration = monthDuration;
    this.nonMonthDuration = nonMonthDuration;
    this.origin = origin;
  }

  protected long dateBin(long interval, long source) {
    long offset = (source - origin) % interval;
    return source - offset;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {

    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        // not do date_bin yet
        if (monthDuration == 0) {
          dateBin(nonMonthDuration, column.getLong(i));
        } else {

        }
        columnBuilder.writeLong(column.getLong(i));
      } else {
        columnBuilder.appendNull();
      }
    }
  }
}
