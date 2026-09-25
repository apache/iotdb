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

package org.apache.iotdb.db.utils;

import org.junit.Assert;
import org.junit.Test;

public class CommonUtilsTest {

  @Test
  public void testIsAliveDoesNotOverflowForLongMinTimestamp() {
    Assert.assertFalse(CommonUtils.isAlive(Long.MIN_VALUE, 1));
    Assert.assertTrue(CommonUtils.isAlive(Long.MAX_VALUE, 1));
    Assert.assertTrue(CommonUtils.isAlive(Long.MIN_VALUE, Long.MAX_VALUE));
  }

  @Test
  public void testTTLLowerBoundDoesNotUnderflowWithHugeTTL() {
    long ttl = Long.MAX_VALUE - 1;
    long ttlLowerBound = CommonUtils.getTTLLowerBound(ttl);

    Assert.assertTrue(ttlLowerBound > Long.MIN_VALUE);
    Assert.assertFalse(CommonUtils.isAlive(Long.MIN_VALUE, ttl));
  }
}
