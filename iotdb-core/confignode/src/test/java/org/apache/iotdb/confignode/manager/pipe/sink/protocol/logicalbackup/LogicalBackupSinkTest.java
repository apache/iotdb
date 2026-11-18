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

package org.apache.iotdb.confignode.manager.pipe.sink.protocol.logicalbackup;

import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupManifest;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupWriter;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;

public class LogicalBackupSinkTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testRecreatedTaskDoesNotDeduplicateANewEvent() throws Exception {
    assertTaskRestartPreservesEvents(10L);
  }

  @Test
  public void testRecreatedTaskDoesNotRejectADifferentEvent() throws Exception {
    assertTaskRestartPreservesEvents(20L);
  }

  private void assertTaskRestartPreservesEvents(final long ttlAfterRestart) throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    final Field writerField = LogicalBackupSink.class.getDeclaredField("writer");
    writerField.setAccessible(true);
    for (int generation = 0; generation < 2; generation++) {
      final PipeConfigRegionWritePlanEvent event =
          new PipeConfigRegionWritePlanEvent(
              new SetTTLPlan(generation == 0 ? 10L : ttlAfterRestart, "root", "backup"),
              "pipe",
              1L,
              null,
              null,
              null,
              null,
              null,
              null,
              true,
              false);
      event.setCommitterKeyAndCommitId(new CommitterKey("pipe", 1L, -1, generation), 1L);
      Assert.assertTrue(event.increaseReferenceCount(getClass().getName()));
      try (final LogicalBackupSink sink = new LogicalBackupSink()) {
        final LogicalBackupManifest manifest = new LogicalBackupManifest();
        manifest.backupId = "backup";
        manifest.pipeName = "pipe";
        manifest.pipeCreationTime = 1L;
        manifest.streamId = "stream";
        manifest.streamType = "config";
        manifest.regionId = -1;
        manifest.timestampPrecision = "ms";
        writerField.set(
            sink,
            new LogicalBackupWriter(
                directory,
                manifest,
                4096,
                1024,
                LogicalBackupWriter.FsyncPolicy.ALWAYS,
                1,
                1,
                generation > 0));
        sink.transfer(event);
        sink.transfer(event);
      } finally {
        event.decreaseReferenceCount(getClass().getName(), false);
      }
    }
    final List<LogicalBackupArchiveReader.BackupStream> streams =
        new LogicalBackupArchiveReader().read(directory, false);
    Assert.assertEquals(1, streams.size());
    Assert.assertEquals(2, streams.get(0).getEventGroups().size());
  }
}
