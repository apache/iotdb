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

package org.apache.tsfile.read.filter.basic;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId;
import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId.NULL;

public abstract class CompareNullFilter extends ValueFilter {

  protected CompareNullFilter(int measurementIndex) {
    super(measurementIndex);
  }

  protected CompareNullFilter(ByteBuffer buffer) {
    super(buffer);
  }

  @Override
  public ClassSerializeId getClassSerializeId() {
    return NULL;
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    super.serialize(outputStream);
  }

  @Override
  public String toString() {
    return String.format("measurements[%s] %s", measurementIndex, getOperatorType().getSymbol());
  }
}
