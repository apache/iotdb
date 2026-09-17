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
package org.apache.iotdb.db.storageengine.dataregion.wal.buffer;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.CheckpointManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.AbstractResultListener.Status;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WALBufferRollRecoveryTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private final List<File> sealedFiles = new CopyOnWriteArrayList<>();
  private NodeStatus previousStatus;
  private String previousReason;
  private File directory;
  private WALBuffer buffer;
  private WALWriter firstWriter;

  @Before
  public void setUp() throws Exception {
    previousStatus = commonConfig.getNodeStatus();
    previousReason = commonConfig.getStatusReason();
    commonConfig.setNodeStatus(NodeStatus.Running);
    directory = temporaryFolder.newFolder("wal");
    buffer =
        new WALBuffer(
            "roll-recovery",
            directory.getPath(),
            new CheckpointManager("roll-recovery", directory.getPath()),
            0,
            0,
            (sealedFile, currentFile) -> sealedFiles.add(sealedFile));
    firstWriter = spy(buffer.currentWALFileWriter);
    buffer.currentWALFileWriter = firstWriter;
  }

  @After
  public void tearDown() throws Exception {
    try {
      // Tests may inject a failure before close(); restore cleanup without reopening any file.
      doCallRealMethod().when(firstWriter).close();
      buffer.close();
    } finally {
      commonConfig.setNodeStatus(previousStatus);
      commonConfig.setStatusReason(previousReason);
    }
  }

  /**
   * Failed successor creation must not reseal the old WAL, inflate counters, or lose later writes.
   */
  @Test
  public void testResumeBeforeWritingAfterRepeatedOpenFailures() throws Exception {
    writeAndAwait(entry(1, 1, "before"), Status.SUCCESS);
    File successor = walFile(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX);
    blockPath(successor);
    roll(Status.FAILURE);
    awaitReadOnly();
    byte[] sealedBytes = Files.readAllBytes(firstWriter.getLogFile().toPath());
    long diskUsage = buffer.getDiskUsage();
    roll(Status.FAILURE);
    assertEquals(0, buffer.getCurrentWALFileVersion());
    assertEquals(1, buffer.getFileNum());
    assertEquals(diskUsage, buffer.getDiskUsage());
    assertTrue(sealedFiles.isEmpty());
    verify(firstWriter, times(1)).close();

    unblockPath(successor);
    commonConfig.setNodeStatus(NodeStatus.Running);
    writeAndAwait(entry(2, 2, "after"), Status.SUCCESS);
    assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
    assertEquals(1, buffer.getCurrentWALFileVersion());
    assertEquals(successor, buffer.currentWALFileWriter.getLogFile());
    assertEquals(2, buffer.getFileNum());
    assertEquals(diskUsage, buffer.getDiskUsage());
    assertEquals(Arrays.asList(firstWriter.getLogFile()), sealedFiles);
    assertArrayEquals(sealedBytes, Files.readAllBytes(firstWriter.getLogFile().toPath()));
    assertEquals(Arrays.asList(1L), readTimes(firstWriter.getLogFile()));
    roll(Status.SUCCESS);
    assertEquals(Arrays.asList(2L), readTimes(successor));
  }

  /** A retry containing only the roll signal should open exactly one successor. */
  @Test
  public void testRollSignalResumesWithoutRollingTwice() throws Exception {
    writeAndAwait(entry(1, 1, "before"), Status.SUCCESS);
    File successor = walFile(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX);
    blockPath(successor);
    roll(Status.FAILURE);
    unblockPath(successor);
    roll(Status.SUCCESS);
    assertEquals(1, buffer.getCurrentWALFileVersion());
    assertEquals(2, buffer.getFileNum());
    assertEquals(1, sealedFiles.size());
    verify(firstWriter, times(1)).close();
  }

  /** Failed rename and then failed open must each resume at the saved stage and notify once. */
  @Test
  public void testResumeRenameThenOpen() throws Exception {
    writeAndAwait(entry(1, -1, "unindexed"), Status.SUCCESS);
    File renamed = walFile(0, 0, WALFileStatus.CONTAINS_NONE_SEARCH_INDEX);
    File successor = walFile(1, 0, WALFileStatus.CONTAINS_SEARCH_INDEX);
    blockPath(renamed);
    roll(Status.FAILURE);
    long diskUsage = buffer.getDiskUsage();
    unblockPath(renamed);
    blockPath(successor);
    roll(Status.FAILURE);
    assertFalse(firstWriter.getLogFile().exists());
    byte[] sealedBytes = Files.readAllBytes(renamed.toPath());
    unblockPath(successor);
    roll(Status.SUCCESS);
    assertEquals(Arrays.asList(renamed), sealedFiles);
    assertArrayEquals(sealedBytes, Files.readAllBytes(renamed.toPath()));
    assertEquals(Arrays.asList(1L), readTimes(renamed));
    assertEquals(diskUsage, buffer.getDiskUsage());
    assertEquals(2, buffer.getFileNum());
    verify(firstWriter, times(1)).close();
  }

  /** Do not append to a partial header or an unexpected nonempty successor left on disk. */
  @Test
  public void testNonemptySuccessorIsNotOverwritten() throws Exception {
    writeAndAwait(entry(1, 1, "before"), Status.SUCCESS);
    File successor = walFile(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX);
    byte[] partialHeader = new byte[] {1, 2};
    Files.write(successor.toPath(), partialHeader);
    roll(Status.FAILURE);
    roll(Status.FAILURE);
    assertArrayEquals(partialHeader, Files.readAllBytes(successor.toPath()));
    assertEquals(0, buffer.getCurrentWALFileVersion());
    verify(firstWriter, times(1)).close();
  }

  /** A failed first chunk must fail the whole large entry; the next batch can recover cleanly. */
  @Test
  public void testFailedSplitEntryDoesNotLeakIntoSuccessor() throws Exception {
    writeAndAwait(entry(1, 1, "before"), Status.SUCCESS);
    File successor = walFile(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX);
    blockPath(successor);
    roll(Status.FAILURE);
    buffer.setBufferSize(192);
    writeAndAwait(entry(2, 2, new String(new char[4096]).replace('\0', 'x')), Status.FAILURE);
    unblockPath(successor);
    writeAndAwait(entry(3, 3, "after"), Status.SUCCESS);
    roll(Status.SUCCESS);
    assertEquals(Arrays.asList(3L), readTimes(successor));
    assertEquals(Arrays.asList(1L), readTimes(firstWriter.getLogFile()));
  }

  /** A failed seal has no proven durable boundary, so an empty successor must not hide it. */
  @Test
  public void testSealFailureIsNotSkipped() throws Exception {
    writeAndAwait(entry(1, 1, "before"), Status.SUCCESS);
    doThrow(new ClosedChannelException()).when(firstWriter).close();
    roll(Status.FAILURE);
    roll(Status.FAILURE);
    assertEquals(0, buffer.getCurrentWALFileVersion());
    assertEquals(1, buffer.getFileNum());
    assertTrue(sealedFiles.isEmpty());
    verify(firstWriter, times(1)).close();
  }

  /** A write failure must never be turned into success by a subsequent force or roll task. */
  @Test
  public void testWriteFailureFailsSubsequentListeners() throws Exception {
    doThrow(new ClosedChannelException())
        .when(firstWriter)
        .write(any(ByteBuffer.class), any(WALMetaData.class));
    writeAndAwait(entry(1, 1, "failed"), Status.FAILURE);
    awaitReadOnly();
    commonConfig.setNodeStatus(NodeStatus.Running);
    roll(Status.FAILURE);
    awaitReadOnly();
    writeAndAwait(entry(2, 2, "also failed"), Status.FAILURE);
    assertEquals(0, buffer.getCurrentWALFileVersion());
    assertTrue(sealedFiles.isEmpty());
    verify(firstWriter, times(1)).write(any(ByteBuffer.class), any(WALMetaData.class));
  }

  private WALInfoEntry entry(long time, long searchIndex, String value) throws Exception {
    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath("root.test.d"),
            false,
            new String[] {"s"},
            new TSDataType[] {TSDataType.TEXT},
            time,
            new Object[] {new Binary(value, TSFileConfig.STRING_CHARSET)},
            false);
    node.setMeasurementSchemas(
        new MeasurementSchema[] {new MeasurementSchema("s", TSDataType.TEXT)});
    node.setSearchIndex(searchIndex);
    return new WALInfoEntry(1, node, false);
  }

  private void writeAndAwait(WALEntry entry, Status expected) {
    buffer.write(entry);
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(expected, entry.getWalFlushListener().waitForResult()));
  }

  private void roll(Status expected) {
    writeAndAwait(new WALSignalEntry(WALEntryType.ROLL_WAL_LOG_WRITER_SIGNAL, false), expected);
  }

  private void awaitReadOnly() {
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(() -> assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus()));
  }

  private File walFile(long version, long searchIndex, WALFileStatus status) {
    return new File(directory, WALFileUtils.getLogFileName(version, searchIndex, status));
  }

  private void blockPath(File path) throws IOException {
    Files.createDirectory(path.toPath());
    // A nonempty directory blocks both file creation and replacement on Windows and Unix.
    Files.write(path.toPath().resolve("blocker"), new byte[] {1});
  }

  private void unblockPath(File path) throws IOException {
    Files.delete(path.toPath().resolve("blocker"));
    Files.delete(path.toPath());
  }

  private List<Long> readTimes(File file) throws IOException {
    List<Long> times = new ArrayList<>();
    try (WALReader reader = new WALReader(file)) {
      while (reader.hasNext()) {
        times.add(((InsertRowNode) reader.next().getValue()).getTime());
      }
    }
    return times;
  }
}
