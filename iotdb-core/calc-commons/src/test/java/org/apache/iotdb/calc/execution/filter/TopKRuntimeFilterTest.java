/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.calc.execution.filter;

import org.junit.Assert;
import org.junit.Test;

public class TopKRuntimeFilterTest {

  @Test
  public void testDescRuntimeFilter() {
    TopKRuntimeFilter filter = new TopKRuntimeFilter(false);
    Assert.assertTrue(filter.mayQualify(100L));
    Assert.assertTrue(filter.mayQualifyRange(0L, Long.MAX_VALUE));

    filter.updateThreshold(50L);
    Assert.assertTrue(filter.mayQualify(60L));
    Assert.assertFalse(filter.mayQualify(50L));
    Assert.assertFalse(filter.mayQualify(40L));

    filter.updateThreshold(55L);
    Assert.assertFalse(filter.mayQualify(54L));
    Assert.assertFalse(filter.mayQualify(55L));
    Assert.assertTrue(filter.mayQualify(56L));
  }

  @Test
  public void testAscRuntimeFilter() {
    TopKRuntimeFilter filter = new TopKRuntimeFilter(true);
    Assert.assertTrue(filter.mayQualify(100L));
    Assert.assertTrue(filter.mayQualifyRange(0L, Long.MAX_VALUE - 1));

    filter.updateThreshold(100L);
    Assert.assertTrue(filter.mayQualify(90L));
    Assert.assertFalse(filter.mayQualify(100L));
    Assert.assertFalse(filter.mayQualify(110L));

    filter.updateThreshold(80L);
    Assert.assertFalse(filter.mayQualify(90L));
    Assert.assertTrue(filter.mayQualify(70L));
  }

  @Test
  public void testMayQualifyRange() {
    TopKRuntimeFilter filter = new TopKRuntimeFilter(false);
    filter.updateThreshold(50L);
    Assert.assertTrue(filter.mayQualifyRange(60L, 100L));
    Assert.assertFalse(filter.mayQualifyRange(50L, 50L));
    Assert.assertFalse(filter.mayQualifyRange(10L, 40L));

    TopKRuntimeFilter ascFilter = new TopKRuntimeFilter(true);
    ascFilter.updateThreshold(100L);
    Assert.assertTrue(ascFilter.mayQualifyRange(10L, 99L));
    Assert.assertFalse(ascFilter.mayQualifyRange(100L, 200L));
  }
}
