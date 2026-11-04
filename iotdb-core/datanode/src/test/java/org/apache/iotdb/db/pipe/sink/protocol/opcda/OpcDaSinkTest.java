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

package org.apache.iotdb.db.pipe.sink.protocol.opcda;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;

public class OpcDaSinkTest {

  private static final String HOLDER = OpcDaSink.class.getName();

  private OpcDaSink sink;
  private OpcDaServerHandle handle;

  @Before
  public void setUp() throws Exception {
    sink = new OpcDaSink();
    handle = Mockito.mock(OpcDaServerHandle.class);
    final Field handleField = OpcDaSink.class.getDeclaredField("handle");
    handleField.setAccessible(true);
    handleField.set(sink, handle);
  }

  @Test
  public void testRawTabletTransfer() throws Exception {
    final PipeRawTabletInsertionEvent event = Mockito.mock(PipeRawTabletInsertionEvent.class);
    final Tablet tablet = Mockito.mock(Tablet.class);
    Mockito.when(event.increaseReferenceCount(HOLDER)).thenReturn(true);
    Mockito.when(event.convertToTablet()).thenReturn(tablet);

    sink.transfer(event);

    Mockito.verify(handle).transfer(tablet);
    Mockito.verify(event).decreaseReferenceCount(HOLDER, false);
  }

  @Test
  public void testInsertNodeTransfersEveryTabletInOrder() throws Exception {
    final PipeInsertNodeTabletInsertionEvent event =
        Mockito.mock(PipeInsertNodeTabletInsertionEvent.class);
    final Tablet first = Mockito.mock(Tablet.class);
    final Tablet second = Mockito.mock(Tablet.class);
    Mockito.when(event.increaseReferenceCount(HOLDER)).thenReturn(true);
    Mockito.when(event.convertToTablets()).thenReturn(Arrays.asList(first, second));

    sink.transfer(event);

    final InOrder order = Mockito.inOrder(handle, event);
    order.verify(handle).transfer(first);
    order.verify(handle).transfer(second);
    order.verify(event).decreaseReferenceCount(HOLDER, false);
  }

  @Test
  public void testReleasedEventsAreNotConvertedOrTransferred() throws Exception {
    final PipeRawTabletInsertionEvent rawEvent = Mockito.mock(PipeRawTabletInsertionEvent.class);
    final PipeInsertNodeTabletInsertionEvent insertEvent =
        Mockito.mock(PipeInsertNodeTabletInsertionEvent.class);

    sink.transfer(rawEvent);
    sink.transfer(insertEvent);

    Mockito.verify(rawEvent, Mockito.never()).convertToTablet();
    Mockito.verify(insertEvent, Mockito.never()).convertToTablets();
    Mockito.verify(rawEvent, Mockito.never()).decreaseReferenceCount(HOLDER, false);
    Mockito.verify(insertEvent, Mockito.never()).decreaseReferenceCount(HOLDER, false);
    Mockito.verifyZeroInteractions(handle);
  }

  @Test
  public void testTransferFailureReleasesBothEventTypesAndPropagates() throws Exception {
    final Tablet tablet = Mockito.mock(Tablet.class);
    final RuntimeException failure = new RuntimeException("transfer failed");
    Mockito.doThrow(failure).when(handle).transfer(tablet);
    final PipeRawTabletInsertionEvent rawEvent = Mockito.mock(PipeRawTabletInsertionEvent.class);
    final PipeInsertNodeTabletInsertionEvent insertEvent =
        Mockito.mock(PipeInsertNodeTabletInsertionEvent.class);
    Mockito.when(rawEvent.convertToTablet()).thenReturn(tablet);
    Mockito.when(insertEvent.convertToTablets()).thenReturn(Collections.singletonList(tablet));

    for (final EnrichedEvent event : Arrays.asList(rawEvent, insertEvent)) {
      Mockito.when(event.increaseReferenceCount(HOLDER)).thenReturn(true);
      try {
        sink.transfer((TabletInsertionEvent) event);
        Assert.fail("Expected the transfer failure");
      } catch (final RuntimeException e) {
        Assert.assertSame(failure, e);
      }
      Mockito.verify(event).decreaseReferenceCount(HOLDER, false);
    }
  }

  @Test
  public void testConversionFailureReleasesBothEventTypesAndPropagates() throws Exception {
    final RuntimeException failure = new RuntimeException("conversion failed");
    final PipeRawTabletInsertionEvent rawEvent = Mockito.mock(PipeRawTabletInsertionEvent.class);
    final PipeInsertNodeTabletInsertionEvent insertEvent =
        Mockito.mock(PipeInsertNodeTabletInsertionEvent.class);
    Mockito.when(rawEvent.convertToTablet()).thenThrow(failure);
    Mockito.when(insertEvent.convertToTablets()).thenThrow(failure);

    for (final EnrichedEvent event : Arrays.asList(rawEvent, insertEvent)) {
      Mockito.when(event.increaseReferenceCount(HOLDER)).thenReturn(true);
      try {
        sink.transfer((TabletInsertionEvent) event);
        Assert.fail("Expected the conversion failure");
      } catch (final RuntimeException e) {
        Assert.assertSame(failure, e);
      }
      Mockito.verify(event).decreaseReferenceCount(HOLDER, false);
    }
    Mockito.verifyZeroInteractions(handle);
  }

  @Test
  public void testUnsupportedEventIsIgnored() throws Exception {
    sink.transfer(Mockito.mock(TabletInsertionEvent.class));

    Mockito.verifyZeroInteractions(handle);
  }
}
