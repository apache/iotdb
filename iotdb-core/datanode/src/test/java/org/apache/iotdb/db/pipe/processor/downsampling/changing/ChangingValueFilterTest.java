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

package org.apache.iotdb.db.pipe.processor.downsampling.changing;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class ChangingValueFilterTest {

  @Test
  public void testExtremeTimestampDistanceReachesMaxInterval() throws Exception {
    final ChangingValueFilter<Integer> filter =
        new ChangingValueFilter<>(createProcessor(0, Long.MAX_VALUE, 0), Long.MIN_VALUE, 0);

    Assert.assertTrue(filter.filter(Long.MAX_VALUE, 0));
  }

  private ChangingValueSamplingProcessor createProcessor(
      final long compressionMinTimeInterval,
      final long compressionMaxTimeInterval,
      final double compressionDeviation)
      throws Exception {
    final ChangingValueSamplingProcessor processor = new ChangingValueSamplingProcessor();
    setField(processor, "compressionMinTimeInterval", compressionMinTimeInterval);
    setField(processor, "compressionMaxTimeInterval", compressionMaxTimeInterval);
    setField(processor, "compressionDeviation", compressionDeviation);
    return processor;
  }

  private void setField(final Object target, final String fieldName, final Object value)
      throws Exception {
    final Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
