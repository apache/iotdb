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

package org.apache.tsfile.read.filter;

import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.tsfile.read.filter.FilterTestUtil.DEFAULT_TIMESTAMP;
import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class NullOperatorsTest {

  @Test
  public void testNull() {
    Filter isNull = ValueFilterApi.isNull(DEFAULT_MEASUREMENT_INDEX);
    Assert.assertTrue(isNull.satisfy(DEFAULT_TIMESTAMP, null));
    Assert.assertFalse(isNull.satisfy(DEFAULT_TIMESTAMP, 100));
  }

  @Test
  public void testNotNull() {
    Filter isNotNull = ValueFilterApi.isNotNull(DEFAULT_MEASUREMENT_INDEX);
    Assert.assertTrue(isNotNull.satisfy(DEFAULT_TIMESTAMP, 100));
    Assert.assertFalse(isNotNull.satisfy(DEFAULT_TIMESTAMP, null));
  }
}
