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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async;

import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class IoTDBDataRegionAsyncSinkTest {

  @Test
  public void testRetryQueueFailureMessageIncludesRootCauseAndIsCleared() {
    final IoTDBDataRegionAsyncSink sink = new IoTDBDataRegionAsyncSink();
    final Event event = Mockito.mock(Event.class);

    sink.addFailureEventToRetryQueue(
        event,
        new PipeException(
            "sink transfer wrapper", new IllegalStateException("receiver rejected request")));

    Assert.assertEquals("receiver rejected request", sink.getLastRetryFailureMessage());
    Assert.assertTrue(
        IoTDBDataRegionAsyncSink.formatRetryQueueFailureMessage(
                1, 1, 0, sink.getLastRetryFailureMessage())
            .contains("receiver rejected request"));

    sink.clearRetryEventsReferenceCount();

    Assert.assertNull(sink.getLastRetryFailureMessage());
    Assert.assertFalse(
        IoTDBDataRegionAsyncSink.formatRetryQueueFailureMessage(
                0, 0, 0, sink.getLastRetryFailureMessage())
            .contains("receiver rejected request"));
  }

  @Test
  public void testRetryQueueFailureMessageKeepsRootCauseTypeWhenMessageIsMissing() {
    final IoTDBDataRegionAsyncSink sink = new IoTDBDataRegionAsyncSink();
    final Event event = Mockito.mock(Event.class);

    sink.addFailureEventToRetryQueue(
        event, new PipeException("sink transfer wrapper", new NullPointerException()));

    Assert.assertEquals("java.lang.NullPointerException", sink.getLastRetryFailureMessage());
  }
}
