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

package org.apache.iotdb.db.queryengine.transformation;

import org.apache.iotdb.db.queryengine.transformation.dag.util.visitor.SubtractionResolver;

import org.apache.tsfile.read.common.type.Type;
import org.junit.Test;

import static org.apache.tsfile.read.common.type.DateType.DATE;
import static org.apache.tsfile.read.common.type.IntType.INT32;

public class TypeResolverTest {

  @Test
  public void test() {
    Type result = SubtractionResolver.checkConditions(DATE, INT32);
    System.out.println(result);
  }
}
