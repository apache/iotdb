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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate;

public class Pair<T1, T2> {

  public final T1 left;
  public final T2 right;

  public Pair(T1 left, T2 right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public final int hashCode() {
    int hashCode = 31 + (left == null ? 0 : left.hashCode());
    return 31 * hashCode + (right == null ? 0 : right.hashCode());
  }

  @Override
  public final boolean equals(Object o) {
    if (!(o instanceof Pair)) {
      return false;
    }
    Pair that = (Pair) o;
    // handles nulls properly
    return equal(left, that.left) && equal(right, that.right);
  }

  // From Apache Licensed guava:
  private boolean equal(Object a, Object b) {
    return a == b || (a != null && a.equals(b));
  }

  @Override
  public String toString() {
    return "(" + left + "," + right + ")";
  }

  public static <X, Y> Pair<X, Y> create(X x, Y y) {
    return new Pair<X, Y>(x, y);
  }
}
