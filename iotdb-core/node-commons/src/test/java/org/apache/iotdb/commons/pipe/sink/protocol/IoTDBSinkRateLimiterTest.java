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

package org.apache.iotdb.commons.pipe.sink.protocol;

import org.apache.iotdb.commons.pipe.sink.limiter.PipeEndPointRateLimiter;

import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

public class IoTDBSinkRateLimiterTest {

  @Test
  public void testRegionTaskDiscardKeepsPipeWideRateLimiter() throws Exception {
    final String pipeName = "rate_limiter_lifecycle_test";
    final long creationTime = System.nanoTime();
    final Pair<String, Long> pipeKey = new Pair<>(pipeName, creationTime);
    final Map<Pair<String, Long>, PipeEndPointRateLimiter> limiters = getRateLimiterMap();
    final PipeEndPointRateLimiter limiter = mock(PipeEndPointRateLimiter.class);
    final IoTDBSink sink = mock(IoTDBSink.class, CALLS_REAL_METHODS);
    limiters.put(pipeKey, limiter);

    try {
      sink.discardEventsOfPipe(pipeName, creationTime, 1);

      assertSame(limiter, limiters.get(pipeKey));

      IoTDBSink.clearPipeRateLimiter(pipeName, creationTime);

      assertFalse(limiters.containsKey(pipeKey));
    } finally {
      IoTDBSink.clearPipeRateLimiter(pipeName, creationTime);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<Pair<String, Long>, PipeEndPointRateLimiter> getRateLimiterMap()
      throws ReflectiveOperationException {
    final Field field = IoTDBSink.class.getDeclaredField("PIPE_END_POINT_RATE_LIMITER_MAP");
    field.setAccessible(true);
    return (Map<Pair<String, Long>, PipeEndPointRateLimiter>) field.get(null);
  }
}
