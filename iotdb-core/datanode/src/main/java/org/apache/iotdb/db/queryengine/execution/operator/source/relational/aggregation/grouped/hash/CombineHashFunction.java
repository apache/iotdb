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
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash;

public final class CombineHashFunction {
  private CombineHashFunction() {}

  public static long getHash(long previousHashValue, long value) {
    return (31 * previousHashValue + value);
  }

  public static void combineAllHashesWithConstant(
      long[] hashes, int fromIndex, int toIndex, long value) {
    // checkFromToIndex(fromIndex, toIndex, hashes.length);

    for (int i = fromIndex; i < toIndex; i++) {
      hashes[i] = (31 * hashes[i]) + value;
    }
  }
}
