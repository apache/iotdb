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

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;

import java.util.List;

public class LeastColumnTransformer extends AbstractGreatestLeastColumnTransformer {
  public LeastColumnTransformer(Type returnType, List<ColumnTransformer> columnTransformerList) {
    super(returnType, columnTransformerList);
  }

  @Override
  protected void transformBoolean(ColumnBuilder builder, List<Boolean> values) {
    returnType.writeBoolean(builder, values.stream().min(Boolean::compareTo).orElse(false));
  }

  @Override
  protected void transformInt(ColumnBuilder builder, List<Integer> values) {
    returnType.writeInt(builder, values.stream().min(Integer::compareTo).orElse(0));
  }

  @Override
  protected void transformLong(ColumnBuilder builder, List<Long> values) {
    returnType.writeLong(builder, values.stream().min(Long::compareTo).orElse(0L));
  }

  @Override
  protected void transformFloat(ColumnBuilder builder, List<Float> values) {
    returnType.writeFloat(builder, values.stream().min(Float::compareTo).orElse(0f));
  }

  @Override
  protected void transformDouble(ColumnBuilder builder, List<Double> values) {
    returnType.writeDouble(builder, values.stream().min(Double::compareTo).orElse(0.0));
  }

  @Override
  protected void transformBinary(ColumnBuilder builder, List<Binary> values) {
    returnType.writeBinary(
        builder, values.stream().min(Binary::compareTo).orElse(BytesUtils.valueOf("")));
  }
}
