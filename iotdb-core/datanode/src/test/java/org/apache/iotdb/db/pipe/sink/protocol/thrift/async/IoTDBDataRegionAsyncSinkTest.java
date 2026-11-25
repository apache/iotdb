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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkResourceException;
import org.apache.iotdb.commons.pipe.resource.PipeResourceFailureType;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.util.Map;

public class IoTDBDataRegionAsyncSinkTest {

  private static class UnbatchedIoTDBDataRegionAsyncSink extends IoTDBDataRegionAsyncSink {

    private UnbatchedIoTDBDataRegionAsyncSink() {
      isTabletBatchModeEnabled = false;
    }
  }

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

  @Test
  public void testConnectionResetIsRetainedAsResourceFailureInRetryQueue() throws Exception {
    final IoTDBDataRegionAsyncSink sink = new IoTDBDataRegionAsyncSink();
    final Event event = Mockito.mock(Event.class);
    final PipeConnectionException failure =
        new PipeConnectionException(
            "transfer failed", new TTransportException(new SocketException("Connection reset")));

    sink.addFailureEventToRetryQueue(event, failure);

    final Method getRetryQueueResourceFailureType =
        IoTDBDataRegionAsyncSink.class.getDeclaredMethod("getRetryQueueResourceFailureType");
    getRetryQueueResourceFailureType.setAccessible(true);
    Assert.assertEquals(
        PipeResourceFailureType.NETWORK_TIMEOUT, getRetryQueueResourceFailureType.invoke(sink));
    sink.clearRetryEventsReferenceCount();
  }

  @Test
  public void testForcedRetryQueueThresholdPreservesTransportResourceFailure() throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final int originalTabletQueueSize = config.getPipeAsyncSinkForcedRetryTabletEventQueueSize();
    final int originalTsFileQueueSize = config.getPipeAsyncSinkForcedRetryTsFileEventQueueSize();
    final int originalTotalQueueSize = config.getPipeAsyncSinkForcedRetryTotalEventQueueSize();
    final long originalRetryExecutionTimeMs =
        config.getPipeAsyncSinkMaxRetryExecutionTimeMsPerCall();
    final IoTDBDataRegionAsyncSink sink = new UnbatchedIoTDBDataRegionAsyncSink();
    try {
      config.setPipeAsyncSinkForcedRetryTabletEventQueueSize(2);
      config.setPipeAsyncSinkForcedRetryTsFileEventQueueSize(1);
      config.setPipeAsyncSinkForcedRetryTotalEventQueueSize(2);
      config.setPipeAsyncSinkMaxRetryExecutionTimeMsPerCall(100);

      final PipeRawTabletInsertionEvent blockedEvent =
          Mockito.mock(PipeRawTabletInsertionEvent.class);
      Mockito.when(blockedEvent.increaseReferenceCount(Mockito.anyString()))
          .thenAnswer(
              invocation -> {
                Thread.sleep(150);
                return false;
              });
      sink.addFailureEventToRetryQueue(blockedEvent, null);

      final PipeRawTabletInsertionEvent resourceFailureEvent =
          Mockito.mock(PipeRawTabletInsertionEvent.class);
      sink.addFailureEventToRetryQueue(resourceFailureEvent, null);

      final Field resourceFailureTypesField =
          IoTDBDataRegionAsyncSink.class.getDeclaredField("retryEvent2ResourceFailureType");
      resourceFailureTypesField.setAccessible(true);
      @SuppressWarnings("unchecked")
      final Map<Event, PipeResourceFailureType> resourceFailureTypes =
          (Map<Event, PipeResourceFailureType>) resourceFailureTypesField.get(sink);
      resourceFailureTypes.put(resourceFailureEvent, PipeResourceFailureType.NETWORK_TIMEOUT);

      final PipeRuntimeSinkResourceException failure =
          Assert.assertThrows(
              PipeRuntimeSinkResourceException.class,
              () -> sink.transfer(new PipeHeartbeatEvent("-1", false)));

      Assert.assertEquals(PipeResourceFailureType.NETWORK_TIMEOUT, failure.getFailureType());
      Assert.assertTrue(failure.isFailureRecorded());
    } finally {
      sink.clearRetryEventsReferenceCount();
      sink.close();
      config.setPipeAsyncSinkForcedRetryTabletEventQueueSize(originalTabletQueueSize);
      config.setPipeAsyncSinkForcedRetryTsFileEventQueueSize(originalTsFileQueueSize);
      config.setPipeAsyncSinkForcedRetryTotalEventQueueSize(originalTotalQueueSize);
      config.setPipeAsyncSinkMaxRetryExecutionTimeMsPerCall(originalRetryExecutionTimeMs);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMissingTsFileRetryLimitKeepsEventQueuedAndStopsSink() throws Exception {
    final IoTDBDataRegionAsyncSink sink = new IoTDBDataRegionAsyncSink();
    final PipeTsFileInsertionEvent event = Mockito.mock(PipeTsFileInsertionEvent.class);
    final File missingTsFile = new File("missing-retry-limit.tsfile").getAbsoluteFile();
    Mockito.when(event.getTsFile()).thenReturn(missingTsFile);

    final Field retryLimitField =
        IoTDBDataRegionAsyncSink.class.getDeclaredField("MAX_FILE_NOT_FOUND_RETRY_TIMES");
    retryLimitField.setAccessible(true);
    final int retryLimit = retryLimitField.getInt(null);

    final Field retryTimesField =
        IoTDBDataRegionAsyncSink.class.getDeclaredField("missingFileRetryTimes");
    retryTimesField.setAccessible(true);
    ((Map<Event, Integer>) retryTimesField.get(sink)).put(event, retryLimit);

    try {
      sink.addFailureEventToRetryQueue(
          event, new FileNotFoundException(missingTsFile.getAbsolutePath()));

      Assert.assertEquals(1, sink.getRetryEventQueueSize());
      Assert.assertTrue(
          sink.getLastRetryFailureMessage().contains(missingTsFile.getAbsolutePath()));
      Assert.assertTrue(sink.getLastRetryFailureMessage().contains(String.valueOf(retryLimit)));
      Mockito.verify(event, Mockito.never())
          .clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());

      try {
        sink.transfer(Mockito.mock(Event.class));
        Assert.fail("The sink should stop transferring after the missing-file retry limit.");
      } catch (final PipeRuntimeSinkCriticalException e) {
        Assert.assertTrue(e.getMessage().contains(missingTsFile.getAbsolutePath()));
      }
    } finally {
      sink.clearRetryEventsReferenceCount();
    }

    Mockito.verify(event).clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());
  }
}
