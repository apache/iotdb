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

package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.util.visitor.BinaryTransformerVisitor;

import static org.apache.tsfile.read.common.type.DateType.DATE;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

public class ArithmeticBinaryColumnTransformerApi {

  public static ColumnTransformer accept(
      BinaryTransformerVisitor visitor,
      ColumnTransformer leftTransformer,
      ColumnTransformer rightTransformer) {
    switch (leftTransformer.getType().getTypeEnum()) {
      case INT32:
        return visitor.visitInt(leftTransformer, rightTransformer);
      case INT64:
        return visitor.visitLong(leftTransformer, rightTransformer);
      case FLOAT:
        return visitor.visitFloat(leftTransformer, rightTransformer);
      case DOUBLE:
        return visitor.visitDouble(leftTransformer, rightTransformer);
      case DATE:
        return visitor.visitDate(leftTransformer, rightTransformer);
      case TIMESTAMP:
        return visitor.visitTimestamp(leftTransformer, rightTransformer);
      default:
        throw new UnsupportedOperationException("Unsupported Type");
    }
  }
}
