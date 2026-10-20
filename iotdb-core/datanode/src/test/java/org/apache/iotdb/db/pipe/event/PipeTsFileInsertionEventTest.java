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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;

public class PipeTsFileInsertionEventTest {

  @Test
  public void testHistoricalModsSurviveOriginalFileRemoval() throws Exception {
    assertHistoricalModsAfterOriginalFileRemoval(true, true);
  }

  @Test
  public void testHistoricalFileWithoutModsSurvivesOriginalFileRemoval() throws Exception {
    assertHistoricalModsAfterOriginalFileRemoval(false, true);
  }

  @Test
  public void testHistoricalModsRemainDisabledAfterOriginalFileRemoval() throws Exception {
    assertHistoricalModsAfterOriginalFileRemoval(true, false);
  }

  private void assertHistoricalModsAfterOriginalFileRemoval(
      final boolean hasMods, final boolean transferMods) throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalMods").toFile();
    final String pipeName = "historical_mods";
    final long creationTime = 1L;
    final String resourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, creationTime);
    final PipeTsFileResourceManager manager = PipeDataNodeResourceManager.tsfile();
    PipeTsFileInsertionEvent event = null;
    boolean increased = false;
    File pinnedTsFile = null;
    File pinnedModFile = null;
    try {
      final File dataDir = new File(tempDir, "unsequence/root.db/0/0");
      Assert.assertTrue(dataDir.mkdirs());
      final TsFileResource resource =
          createNonEmptyTsFileResource(dataDir, "1-1-0-0.tsfile", 1L, 0);
      resource.setStatusForTest(TsFileResourceStatus.NORMAL);
      final Deletion deletion = new Deletion(new PartialPath("root.db.d0.s1"), 0, 1L, 1L);
      if (hasMods) {
        resource.getModFile().write(deletion);
        resource.getModFile().close();
      }
      manager.pinTsFileResource(resource, transferMods, resourcePipeName);
      pinnedTsFile =
          PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
              resource.getTsFile(), resourcePipeName);
      pinnedModFile = new File(pinnedTsFile + ModificationFile.FILE_SUFFIX);

      // Compaction removes the original after historical selection has pinned both files.
      Assert.assertTrue(resource.remove());
      Assert.assertFalse(resource.modFileExists());
      Assert.assertEquals(hasMods && transferMods, pinnedModFile.exists());
      event =
          new PipeTsFileInsertionEvent(
              resource,
              null,
              transferMods,
              false,
              true,
              pipeName,
              creationTime,
              null,
              null,
              Long.MIN_VALUE,
              Long.MAX_VALUE);
      increased = event.internallyIncreaseResourceReferenceCount(getClass().getName());
      Assert.assertTrue(increased);
      Assert.assertEquals(hasMods && transferMods, event.isWithMod());
      if (hasMods && transferMods) {
        Assert.assertEquals(pinnedModFile, event.getModFile());
        try (final ModificationFile mods = new ModificationFile(event.getModFile().getPath())) {
          Assert.assertEquals(
              Collections.singletonList(deletion), new ArrayList<>(mods.getModifications()));
        }
      } else {
        Assert.assertNull(event.getModFile());
      }
    } finally {
      if (increased) {
        event.internallyDecreaseResourceReferenceCount(getClass().getName());
      } else if (event != null) {
        event.close();
      }
      if (pinnedModFile != null) {
        manager.decreaseFileReference(pinnedModFile, resourcePipeName);
      }
      if (pinnedTsFile != null) {
        manager.decreaseFileReference(pinnedTsFile, resourcePipeName);
      }
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test(timeout = 60000)
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
