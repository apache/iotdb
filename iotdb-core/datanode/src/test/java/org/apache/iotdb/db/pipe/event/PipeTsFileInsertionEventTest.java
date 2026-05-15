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

package org.apache.iotdb.db.pipe.event;

import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class PipeTsFileInsertionEventTest {

  @Test(timeout = 5000)
  public void testRealtimeEventCanSkipWaitingForClosedStatusAfterTsFileSealed() throws Exception {
    final File tempDir = Files.createTempDirectory("pipeTsFileSealed").toFile();

    try {
      final TsFileResource resource =
          createNonEmptyTsFileResource(tempDir, "realtime.tsfile", 1L, 1);
      Assert.assertFalse(resource.isClosed());
      Assert.assertFalse(resource.isEmpty());

      final PipeTsFileInsertionEvent sourceEvent = new PipeTsFileInsertionEvent(resource, false);
      Assert.assertTrue(sourceEvent.waitForTsFileClose());

      final PipeTsFileInsertionEvent copiedEvent =
          sourceEvent.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
              "pipe", 1L, null, null, Long.MIN_VALUE, Long.MAX_VALUE);
      Assert.assertTrue(copiedEvent.waitForTsFileClose());

      copiedEvent.close();
      sourceEvent.close();
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  private TsFileResource createNonEmptyTsFileResource(
      final File tempDir, final String fileName, final long flushOrderId, final int dataRegionId)
      throws IOException {
    final File file = new File(tempDir, fileName);
    Assert.assertTrue(file.createNewFile());

    final TsFileResource resource = new TsFileResource(file);
    resource.updateProgressIndex(new SimpleProgressIndex(1, flushOrderId));
    final ITimeIndex timeIndex = new DeviceTimeIndex();
    final IDeviceID deviceID = new PlainDeviceID("root.db.d" + dataRegionId);
    timeIndex.putStartTime(deviceID, 1);
    timeIndex.putEndTime(deviceID, 1);
    resource.setTimeIndex(timeIndex);
    return resource;
  }
}
