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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipeRawTabletInsertionEventTest {

  @Test
  public void testFailedParsedTabletAbortsSourceProgressRegardlessOfReleaseOrder() {
    assertFailedParsedTabletAbortsSourceProgress(true);
    assertFailedParsedTabletAbortsSourceProgress(false);
  }

  private static void assertFailedParsedTabletAbortsSourceProgress(
      final boolean releaseFailedTabletFirst) {
    final TestPipeTsFileInsertionEvent sourceEvent = createProgressManagedSourceEvent();
    final PipeRawTabletInsertionEvent failedTablet = createEvent(sourceEvent, false);
    final PipeRawTabletInsertionEvent successfulTablet = createEvent(sourceEvent, true);

    Assert.assertFalse(failedTablet.needToCommit());
    Assert.assertFalse(successfulTablet.needToCommit());
    Assert.assertTrue(sourceEvent.increaseReferenceCount("processor"));
    Assert.assertTrue(failedTablet.increaseReferenceCount("collector"));
    Assert.assertTrue(successfulTablet.increaseReferenceCount("collector"));
    Assert.assertEquals(3, sourceEvent.getReferenceCount());

    sourceEvent.decreaseReferenceCount("processor", true);
    if (releaseFailedTabletFirst) {
      failedTablet.clearReferenceCount("discarded");
      successfulTablet.decreaseReferenceCount("transferred", true);
    } else {
      successfulTablet.decreaseReferenceCount("transferred", true);
      failedTablet.clearReferenceCount("discarded");
    }

    Assert.assertTrue(sourceEvent.isReleased());
    Assert.assertFalse(sourceEvent.needToCommit());
  }

  private static TestPipeTsFileInsertionEvent createProgressManagedSourceEvent() {
    final File tsFile = new File("target/source-progress.tsfile");
    final TsFileResource resource = mock(TsFileResource.class);
    when(resource.getTsFile()).thenReturn(tsFile);
    when(resource.isClosed()).thenReturn(true);

    final TestPipeTsFileInsertionEvent sourceEvent =
        new TestPipeTsFileInsertionEvent(resource, tsFile);
    sourceEvent.markProgressReportManagedByTsFileParser();
    return sourceEvent;
  }

  private static PipeRawTabletInsertionEvent createEvent(
      final EnrichedEvent sourceEvent, final boolean needToReport) {
    final MeasurementSchema schema = new MeasurementSchema("s", TSDataType.INT64);
    final Tablet tablet = new Tablet("root.sg.d", Collections.singletonList(schema), 1);
    tablet.addTimestamp(0, 1);
    tablet.addValue("s", 0, 1L);
    return new PipeRawTabletInsertionEvent(
        null, null, null, null, tablet, false, "pipe", 1, null, sourceEvent, needToReport);
  }

  private static class TestPipeTsFileInsertionEvent extends PipeTsFileInsertionEvent {

    private TestPipeTsFileInsertionEvent(final TsFileResource resource, final File tsFile) {
      super(
          null, null, resource, tsFile, false, false, false, null, "pipe", 1, null, null, null,
          null, null, null, true, 0, 1);
    }

    @Override
    public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
      return true;
    }

    @Override
    public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
      return true;
    }
  }
}
