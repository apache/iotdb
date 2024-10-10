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

package org.apache.iotdb.db.queryengine.transformation.dag.column;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.AbstractIntType;
import org.apache.tsfile.read.common.type.AbstractLongType;
import org.apache.tsfile.read.common.type.AbstractVarcharType;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;

public class TreeCaseWhenThenColumnTransformer extends AbstractCaseWhenThenColumnTransformer {

  public TreeCaseWhenThenColumnTransformer(
      Type returnType,
      List<ColumnTransformer> whenTransformers,
      List<ColumnTransformer> thenTransformers,
      ColumnTransformer elseTransformer) {
    super(returnType, whenTransformers, thenTransformers, elseTransformer);
  }

  @Override
  protected void writeToColumnBuilder(
      Type thenColumnType, Column column, int index, ColumnBuilder builder) {
    if (returnType instanceof BooleanType) {
      builder.writeBoolean(thenColumnType.getBoolean(column, index));
    } else if (returnType instanceof AbstractIntType) {
      builder.writeInt(thenColumnType.getInt(column, index));
    } else if (returnType instanceof AbstractLongType) {
      builder.writeLong(thenColumnType.getLong(column, index));
    } else if (returnType instanceof FloatType) {
      builder.writeFloat(thenColumnType.getFloat(column, index));
    } else if (returnType instanceof DoubleType) {
      builder.writeDouble(thenColumnType.getDouble(column, index));
    } else if (returnType instanceof AbstractVarcharType || returnType instanceof BlobType) {
      builder.writeBinary(thenColumnType.getBinary(column, index));
    } else {
      throw new UnsupportedOperationException("Unsupported Type");
    }
  }
}
