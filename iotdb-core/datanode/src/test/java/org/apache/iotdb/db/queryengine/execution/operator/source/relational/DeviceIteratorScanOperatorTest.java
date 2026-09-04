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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeviceIteratorScanOperatorTest {

  @Test
  public void testMemoryEstimationBeforeFetchingDeviceEntry() {
    OperatorContext operatorContext = mock(OperatorContext.class);
    DeviceIteratorScanOperator.DeviceChildOperatorTreeGenerator generator =
        mock(DeviceIteratorScanOperator.DeviceChildOperatorTreeGenerator.class);
    when(generator.calculateMaxPeekMemory()).thenReturn(11L);
    when(generator.calculateMaxReturnSize()).thenReturn(12L);
    when(generator.calculateRetainedSizeAfterCallingNext()).thenReturn(13L);

    DeviceIteratorScanOperator operator =
        new DeviceIteratorScanOperator(operatorContext, Collections.emptyList(), generator);

    assertEquals(11L, operator.calculateMaxPeekMemory());
    assertEquals(12L, operator.calculateMaxReturnSize());
    assertEquals(13L, operator.calculateRetainedSizeAfterCallingNext());
  }
}
