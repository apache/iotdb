/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.ChunkOffsetCalculator;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkPayloadRef;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkPayloadUnavailableException;
import org.apache.iotdb.db.storageengine.load.splitter.NonAlignedChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.TsFilePrecalculatedChunkWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LoadTsFileManagerTest {

  /**
   * The envelope a serialized piece carries in front of itself when it travels as a consensus
   * request or a WAL entry: the entry type, a sentinel mem-table id and the plan node type.
   */
  private static final int DISPATCH_ENVELOPE_BYTES = 11;

  private File tempDir;
  private String[] originalLoadBaseDirs;
  private DataRegion dataRegion;
  private IoTDBConfig config;

  /** The watermark this test reports through the stubbed WAL node, which the retention reads. */
  private final AtomicLong walWatermark = new AtomicLong();

  /** The release listener the retention registers, so that the test can fire it. */
  private final AtomicReference<LongConsumer> walWatermarkListener = new AtomicReference<>();

  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("load-tsfile-manager-test").toFile();
    config = IoTDBDescriptor.getInstance().getConfig();
    originalLoadBaseDirs = config.getLoadTsFileDirs();
    config.setLoadTsFileDirs(new String[] {tempDir.getAbsolutePath()});

    dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn("root.load_manager_test");
    Mockito.when(dataRegion.getDataRegionIdString()).thenReturn("0");
    Mockito.when(dataRegion.getNonSystemDatabaseName())
        .thenReturn(Optional.of("root.load_manager_test"));
  }

  @After
  public void tearDown() throws Exception {
    config.setLoadTsFileDirs(originalLoadBaseDirs);
    deleteRecursively(tempDir);
  }

  @Test
  public void testOutOfOrderPieceWriteKeepsProgressAndDataReadable() throws Exception {
    final String uuid = "test-uuid";
    final int pointCount = 100;
    final int deviceCount = 5;
    final int measurementCount = 4;
    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final List<ChunkData> earlierChunks = new ArrayList<>();
    final List<ChunkData> laterChunks = new ArrayList<>();
    final List<SeriesExpectation> expectations = new ArrayList<>();
    int valueBase = 0;

    for (int deviceIndex = 0; deviceIndex < deviceCount; deviceIndex++) {
      final StringArrayDeviceID device =
          new StringArrayDeviceID("root", "load_manager_test", "d" + deviceIndex);
      for (int measurementIndex = 0; measurementIndex < measurementCount; measurementIndex++) {
        final String measurement = "s" + measurementIndex;
        final NonAlignedChunkData chunkData =
            createNonAlignedChunkData(device, measurement, valueBase, pointCount);
        calculator.assign(chunkData);
        if (deviceIndex < 2) {
          earlierChunks.add(chunkData);
        } else {
          laterChunks.add(chunkData);
        }
        expectations.add(
            new SeriesExpectation(device, measurement, pointCount, valueBase, valueBase + 99));
        valueBase += pointCount;
      }
    }

    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    File tsFile = null;
  }

  /**
   * What a DataNode restart leaves behind in the middle of an out-of-order load: one piece is
   * staged, every in-memory object of the task goes away, a new manager picks the staged directory
   * up from its progress files, the piece the hole belongs to arrives afterwards - and the file
   * that is finally imported has to be complete and readable.
   */
  @Test
  public void testOutOfOrderPiecesSurviveTheLossOfTheInMemoryWriters() throws Exception {
    final String uuid = "restarted-load";
    final int pointCount = 100;
    final int deviceCount = 5;
    final int measurementCount = 4;
    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final List<ChunkData> earlierChunks = new ArrayList<>();
    final List<ChunkData> laterChunks = new ArrayList<>();
    final List<SeriesExpectation> expectations = new ArrayList<>();
    int valueBase = 0;

    for (int deviceIndex = 0; deviceIndex < deviceCount; deviceIndex++) {
      final StringArrayDeviceID device =
          new StringArrayDeviceID("root", "load_manager_test", "d" + deviceIndex);
      for (int measurementIndex = 0; measurementIndex < measurementCount; measurementIndex++) {
        final String measurement = "s" + measurementIndex;
        final NonAlignedChunkData chunkData =
            createNonAlignedChunkData(device, measurement, valueBase, pointCount);
        calculator.assign(chunkData);
        if (deviceIndex < 2) {
          earlierChunks.add(chunkData);
        } else {
          laterChunks.add(chunkData);
        }
        expectations.add(
            new SeriesExpectation(device, measurement, pointCount, valueBase, valueBase + 99));
        valueBase += pointCount;
      }
    }

    emulateStagedFileImport();

    // The later piece arrives first and is staged, leaving the hole that the earlier piece owns.
    final LoadTsFileManager firstRun = new LoadTsFileManager(dataRegion);
    final List<LoadTsFileConsensusNode.PieceRef> laterRefs =
        firstRun.writePiece(uuid, toTsFileDataList(laterChunks));
    assertEquals(1, laterRefs.size());
    final File staged = new File(tempDir, laterRefs.get(0).getRelativePath());
    assertTrue(staged.isFile());
    assertTrue(staged.length() > 0L);

    // The in-memory objects of the task are dropped the way a restart drops them: their buffered
    // bytes are flushed, the objects themselves are gone, and only the staged file with its
    // progress records stays on disk.
    dropInMemoryWriters(firstRun);

    // The first lookup of a staging directory makes the tier manager reformulate the configured
    // LOAD directories from the data directories, which in a test is not the directory the pieces
    // were just staged in. A DataNode does that once at startup, before any load, so pinning the
    // directory here is what keeps the restarted manager looking at the very same staged file.
    config.setLoadTsFileDirs(new String[] {tempDir.getAbsolutePath()});

    // A new manager resumes the very same staged directory from those records.
    final LoadTsFileManager recovered = new LoadTsFileManager(dataRegion);

    // The piece that owned the hole reaches the node after the restart.
    recovered.writePiece(uuid, toTsFileDataList(earlierChunks));
    final LoadTsFileProgress stagedProgress = new LoadTsFileProgress(staged);
    assertEquals(deviceCount * measurementCount, stagedProgress.readAllRecords().size());
    assertTrue(stagedProgress.isReady(staged.length()));
    assertTrue(recovered.prepare(prepareNode(uuid), Collections.emptyMap()));

    final LoadTsFileConsensusNode commit = commitNode(uuid);
    commit.setSearchIndex(10L);
    assertTrue(recovered.loadAll(commit, Collections.emptyMap()));

    final File imported = new File(new File(tempDir, "data"), staged.getName());
    assertTrue("the resumed task must have been imported", imported.isFile());

    // The point of the whole exercise: the imported TsFile carries every series completely, so the
    // data staged before the restart and the data staged after it are both readable.
    assertAllSeriesReadable(imported, expectations);
  }

  private static void dropInMemoryWriters(final LoadTsFileManager manager) throws Exception {
    final Field writersField = LoadTsFileManager.class.getDeclaredField("uuid2WriterManager");
    writersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, TsFileWriterManager> writers =
        (Map<String, TsFileWriterManager>) writersField.get(manager);
    for (final TsFileWriterManager writerManager : writers.values()) {
      final Field partitionsField =
          TsFileWriterManager.class.getDeclaredField("dataPartition2Writer");
      partitionsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      final Map<?, TsFilePrecalculatedChunkWriter> partitionWriters =
          (Map<?, TsFilePrecalculatedChunkWriter>) partitionsField.get(writerManager);
      for (final TsFilePrecalculatedChunkWriter writer : partitionWriters.values()) {
        writer.getOutput().flush();
      }
    }
    writers.clear();
  }

  @Test
  public void testPrepareWithoutStagedDataFails() throws Exception {
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    // A replica that only received the piece references allocates the staged directory of the load
    // without writing anything into it, so sealing such a load must not report success.
    manager.writePiece("empty-load", Collections.emptyList());

    try {
      manager.prepare(prepareNode("empty-load"), Collections.emptyMap());
      fail("prepare must not succeed when the load has no staged data");
    } catch (final LoadFileException e) {
      assertTrue(e.getMessage().contains("empty-load"));
    } finally {
      manager.deleteAll(abortNode("empty-load"));
    }
  }

  /**
   * A chunk payload reference that cannot describe bytes of any staged file is rejected where it is
   * built or deserialized. Reading one back would otherwise allocate a negative or overflowing
   * buffer, or seek to a negative offset, and fail with an unchecked exception that no caller of
   * the consensus path expects.
   */
  @Test
  public void testChunkPayloadRefRejectsMalformedReferences() throws Exception {
    final String path = "/tmp/staged.tsfile";
    final long[][] invalidBounds = {
      {-1L, 10L},
      {0L, -1L},
      {0L, (long) Integer.MAX_VALUE + 1L},
      {(long) Integer.MAX_VALUE, 10L}
    };
    for (final long[] bounds : invalidBounds) {
      try {
        new ChunkPayloadRef(path, bounds[0], bounds[1]);
        fail("a reference of offset " + bounds[0] + " and size " + bounds[1] + " must be rejected");
      } catch (final IllegalArgumentException e) {
        assertTrue(e.getMessage().contains(path));
      }
    }
    try {
      new ChunkPayloadRef(null, 0L, 1L);
      fail("a reference without a file must be rejected");
    } catch (final IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("null"));
    }

    // The same reference arriving over the wire is malformed input rather than a programming error:
    // deserializing it has to fail as an I/O failure of the request that carries it.
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final DataOutputStream out = new DataOutputStream(bytes)) {
      ReadWriteIOUtils.write(path, out);
      ReadWriteIOUtils.write(0L, out);
      ReadWriteIOUtils.write(-1L, out);
    }
    try {
      ChunkPayloadRef.deserializeFrom(new ByteArrayInputStream(bytes.toByteArray()));
      fail("deserializing a malformed reference must fail");
    } catch (final IOException e) {
      assertTrue(e.getMessage().contains(path));
    }
  }

  @Test
  public void testChunkPayloadRefRefusesToReadBeyondTheStagedFile() throws Exception {
    final String loadId = "payload-ref-load";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    final NonAlignedChunkData chunkData = laidOutChunk();
    final LoadTsFileConsensusNode piece = stagedPiece(loadId, chunkData);
    manager.writePiece(piece);
    final File staged = new File(tempDir, piece.getPieceRefs().get(0).getRelativePath());
    final ChunkPayloadRef valid = chunkData.getChunkPayloadRefs().get(0);
    assertTrue(valid.readPayload().length > 0);

    // The bytes this reference names are past the end of the staged file, so there is nothing to
    // read: the reference is refused before its payload is allocated.
    final ChunkPayloadRef pastTheEnd =
        new ChunkPayloadRef(valid.getFilePath(), valid.getOffset(), staged.length());
    try {
      pastTheEnd.readPayload();
      fail("reading past the end of the staged file must fail");
    } catch (final ChunkPayloadUnavailableException e) {
      assertTrue(e.getMessage().contains(staged.getAbsolutePath()));
    }
  }

  /**
   * A PREPARE of a task that is still missing a piece must fail instead of sealing the file.
   *
   * <p>Only the second of the two chunks arrives here, and it is staged at the offset its own
   * content defines, past the bytes the first one owns. The staged file is then exactly as long as
   * the last recorded chunk, so its length says nothing about the hole in the middle of it and the
   * completeness check has to look at the recorded ranges.
   */
  @Test
  public void testPrepareFailsWhileAPieceOfTheTaskIsStillMissing() throws Exception {
    final String loadId = "holey-load";
    final NonAlignedChunkData[] chunks = twoChunksOfTheSamePartition();
    final NonAlignedChunkData first = chunks[0];
    final NonAlignedChunkData second = chunks[1];

    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    final List<LoadTsFileConsensusNode.PieceRef> refs =
        manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(second)));
    final File staged = new File(tempDir, refs.get(0).getRelativePath());
    assertTrue(staged.isFile());

    final LoadTsFileProgress progress = new LoadTsFileProgress(staged);
    // The records of a progress file are read on demand, so the lengths below need them loaded.
    progress.readAllRecords();
    assertEquals(
        second.getChunkLayout().offset() + second.getChunkLayout().length(), staged.length());
    assertEquals(staged.length(), progress.getTotalLength());
    assertFalse(progress.isReady(staged.length()));
    try {
      manager.prepare(prepareNode(loadId), Collections.emptyMap());
      fail("PREPARE must not seal a task whose piece never arrived");
    } catch (final LoadFileException e) {
      assertTrue(e.getMessage().contains(staged.getAbsolutePath()));
    }

    // The piece that owned the hole arrives and is written where the layout puts it, so the two
    // chunks cover the file from its header on without a gap and the same task can be sealed.
    manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(first)));
    // The records the getters below read are the ones this instance has loaded, so they are read
    // again after the piece was staged.
    progress.readAllRecords();
    assertEquals(-1L, progress.getFirstHoleOffset());
    assertEquals(
        second.getChunkLayout().offset() + second.getChunkLayout().length(), staged.length());
    assertTrue(progress.isReady(staged.length()));
    assertTrue(manager.prepare(prepareNode(loadId), Collections.emptyMap()));
  }

  /**
   * A progress file that ends in the middle of an entry is what a snapshot of the staging directory
   * holds when it was taken while a piece was being appended. The fragment describes nothing, while
   * the entries before it are intact and describe bytes that are on disk, so recovery drops the
   * fragment and resumes the staged file with those entries instead of treating the whole file as
   * unattributable.
   */
  @Test
  public void testTornProgressEntryIsRepairedOnRecovery() throws Exception {
    final String loadId = "torn-progress";
    final NonAlignedChunkData[] chunks = twoChunksOfTheSamePartition();
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    final List<LoadTsFileConsensusNode.PieceRef> firstRefs =
        manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(chunks[0])));
    final File staged = new File(tempDir, firstRefs.get(0).getRelativePath());
    manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(chunks[1])));
    final long lengthAfterBothPieces = staged.length();
    final File progressFile = LoadTsFileProgress.progressFileFor(staged);

    // The copy was taken while the entry of the second piece was still being appended.
    try (final FileChannel channel =
        FileChannel.open(progressFile.toPath(), StandardOpenOption.WRITE)) {
      channel.truncate(progressFile.length() - 5L);
    }

    // A restart resumes the staged file from the entries that are complete, and the piece of the
    // fragment is sent again, which is what a replica that joined through the snapshot sees. It is
    // a
    // fresh piece with the payload in memory: the offsets of a chunk come from its own content, so
    // laying both chunks out again puts this one at the very offset it had before.
    final LoadTsFileManager recovered = new LoadTsFileManager(dataRegion);
    final NonAlignedChunkData replayedChunk = twoChunksOfTheSamePartition()[1];
    final List<LoadTsFileConsensusNode.PieceRef> replayedRefs =
        recovered.writePiece(loadId, toTsFileDataList(Collections.singletonList(replayedChunk)));
    assertFalse("the re-sent piece must have been staged", replayedRefs.isEmpty());
    assertEquals("both pieces have to be staged again", lengthAfterBothPieces, staged.length());

    final LoadTsFileProgress progress = new LoadTsFileProgress(staged);
    progress.readAllRecords();
    assertTrue(progress.isReady(staged.length()));
    assertTrue(recovered.prepare(prepareNode(loadId), Collections.emptyMap()));
  }

  /**
   * A staged file that no writer could resume cannot take the chunks of a further piece: their
   * offsets belong to that file, which already exists. The piece has to fail instead of being
   * dropped, because a replica that loses a piece silently imports a file that misses it.
   */
  @Test
  public void testPieceOfAStagedFileWithoutResumableWriterFails() throws Exception {
    final String loadId = "unresumable";
    final File regionDir = new File(tempDir, "root.load_manager_test-0");
    final File taskDir = new File(regionDir, loadId);
    assertTrue(taskDir.mkdirs());
    // A staged file without any progress file: nothing says which chunks its bytes hold.
    final File staged = new File(taskDir, "root.load_manager_test-0-0.tsfile");
    Files.write(staged.toPath(), new byte[] {1, 2, 3, 4, 5, 6, 7});

    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    try {
      manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(laidOutChunk())));
      fail("a piece of a staged file that cannot be resumed must fail");
    } catch (final IOException e) {
      assertTrue(e.getMessage().contains(staged.getPath()));
      assertTrue(e.getMessage().contains(loadId));
    }
  }

  /** Two chunks of one time partition, the second one laid out behind the first one. */
  private static NonAlignedChunkData[] twoChunksOfTheSamePartition() {
    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final NonAlignedChunkData first =
        createNonAlignedChunkData(
            new StringArrayDeviceID("root", "load_manager_test", "d0"), "s0", 0, 10);
    final NonAlignedChunkData second =
        createNonAlignedChunkData(
            new StringArrayDeviceID("root", "load_manager_test", "d1"), "s1", 100, 10);
    calculator.assign(first);
    calculator.assign(second);
    return new NonAlignedChunkData[] {first, second};
  }

  @Test
  public void testAbortedConsensusTaskReleasesStagedFilesWhenNoWatermarkIsReported()
      throws Exception {
    // Without a consensus watermark (single replica) nothing can read the staged bytes back, so the
    // directory of a finished task is deleted right away.
    final String loadId = "aborted-load";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    final List<LoadTsFileConsensusNode.PieceRef> refs =
        manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(laidOutChunk())));
    final File staged = new File(tempDir, refs.get(0).getRelativePath());
    assertTrue(staged.isFile());

    assertTrue(
        manager.deleteAll(
            LoadTsFileConsensusNode.abort(new PlanNodeId("abort"), loadId, "file-1", false)));

    assertFalse(staged.exists());
    assertFalse(staged.getParentFile().exists());
  }

  @Test
  public void testFinishedConsensusTaskKeepsStagedFilesUntilTheWatermarkAdvances()
      throws Exception {
    final WALMode originalWALMode = config.getWalMode();
    config.setWalMode(WALMode.SYNC);
    try (final WALNode walNode =
        new WALNode("load-retention-test", new File(tempDir, "wal").getAbsolutePath())) {
      Mockito.when(dataRegion.getWALNode()).thenReturn(Optional.of(walNode));
      // The watermark lags behind the command, so a follower may still need the staged bytes.
      walNode.setSafelyDeletedSearchIndex(5L);

      final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
      final String loadId = "retained-load";
      final List<LoadTsFileConsensusNode.PieceRef> refs =
          manager.writePiece(loadId, toTsFileDataList(Collections.singletonList(laidOutChunk())));
      final File staged = new File(tempDir, refs.get(0).getRelativePath());
      assertTrue(staged.isFile());

      final LoadTsFileConsensusNode abort =
          LoadTsFileConsensusNode.abort(new PlanNodeId("abort"), loadId, "file-1", false);
      abort.setSearchIndex(10L);
      assertTrue(manager.deleteAll(abort));

      // The staged bytes survive for a lagging follower, and the progress files are gone so that a
      // restart does not resume the finished task.
      assertTrue(staged.isFile());
      assertTrue(new File(staged.getParentFile(), "terminal.marker").isFile());
      for (final File file : staged.getParentFile().listFiles()) {
        assertFalse(file.getName().endsWith(".progress"));
      }

      // Once every follower got past the command, the directory is released.
      walNode.setSafelyDeletedSearchIndex(10L);
      assertFalse(staged.exists());
      assertFalse(staged.getParentFile().exists());
    } finally {
      config.setWalMode(originalWALMode);
    }
  }

  @Test
  public void testStagedDirectoryOfFinishedTaskIsDeletedOnRecovery() throws Exception {
    // A finished task keeps its staged directory with a terminal marker; a restart must delete it
    // instead of resuming it.
    final File regionDir = new File(tempDir, "root.load_manager_test-0");
    final File taskDir = new File(regionDir, "finished-load");
    assertTrue(taskDir.mkdirs());
    Files.write(new File(taskDir, "123.tsfile").toPath(), new byte[] {1, 2, 3});
    Files.write(
        new File(taskDir, "terminal.marker").toPath(), "7".getBytes(StandardCharsets.UTF_8));

    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);

    assertFalse(taskDir.exists());
    assertFalse(manager.prepare(prepareNode("finished-load"), Collections.emptyMap()));
  }

  @Test
  public void testStagedDirectoryWithoutMarkerIsKeptOnRecovery() throws Exception {
    // A directory without a terminal marker may belong to an in-progress task, so recovery must
    // leave it on disk.
    final File regionDir = new File(tempDir, "root.load_manager_test-0");
    final File taskDir = new File(regionDir, "in-progress-load");
    assertTrue(taskDir.mkdirs());
    Files.write(new File(taskDir, "123.tsfile").toPath(), new byte[] {1, 2, 3});

    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);

    assertTrue(taskDir.exists());
    assertFalse(manager.prepare(prepareNode("in-progress-load"), Collections.emptyMap()));
  }

  @Test
  public void testCommitKeepsTheStagedBytesWhileAReplicaMayStillNeedThem() throws Exception {
    // The watermark lags behind the COMMIT command, so a replica that catches up from this node's
    // WAL still has to expand the pieces of the task from the staging directory.
    useWatermarkWALNode(5L);
    emulateStagedFileImport();

    final String loadId = "committed-load";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    final NonAlignedChunkData chunkData = laidOutChunk();
    final LoadTsFileConsensusNode piece = stagedPiece(loadId, chunkData);
    manager.writePiece(piece);
    final File staged = new File(tempDir, piece.getPieceRefs().get(0).getRelativePath());
    final ChunkPayloadRef payloadRef = chunkData.getChunkPayloadRefs().get(0);
    final byte[] stagedPayload = payloadRef.readPayload();
    assertTrue(stagedPayload.length > 0);
    assertTrue(manager.prepare(prepareNode(loadId), Collections.emptyMap()));

    final LoadTsFileConsensusNode commit = commitNode(loadId);
    commit.setSearchIndex(10L);
    assertTrue(manager.loadAll(commit, Collections.emptyMap()));

    // The import must have been made from a copy. The WAL entry of every piece of the task only
    // references the staged bytes, and those entries are expanded on this node to serve a replica
    // that has not applied them yet, so taking the staged file away would leave them unexpandable.
    assertTrue("COMMIT must not take the referenced bytes away", staged.isFile());
    assertArrayEquals(stagedPayload, payloadRef.readPayload());

    // The reference-only form of the piece still expands to the piece the leader applied.
    final byte[] forwarded = toByteArray(piece.serialize());
    final byte[] reference = toByteArray(piece.serialize(false));
    final LoadTsFileConsensusNode restored =
        LoadTsFileConsensusNode.deserializeFromWAL(
            ByteBuffer.wrap(
                    reference, DISPATCH_ENVELOPE_BYTES, reference.length - DISPATCH_ENVELOPE_BYTES)
                .slice());
    assertArrayEquals(forwarded, toByteArray(restored.serialize()));

    // Once every replica got past the COMMIT, the staged bytes are deleted.
    advanceWatermark(10L);
    assertFalse(staged.exists());
    assertFalse(staged.getParentFile().exists());
  }

  @Test
  public void testCommitTakesTheStagedFilesAwayOnceEveryReplicaPassedTheCommand() throws Exception {
    // The watermark already got past the COMMIT command, so no replica can read the staged bytes
    // back any more: the import may take the staged file itself and nothing is kept.
    useWatermarkWALNode(20L);
    emulateStagedFileImport();

    final String loadId = "not-retained-load";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    final NonAlignedChunkData chunkData = laidOutChunk();
    final LoadTsFileConsensusNode piece = stagedPiece(loadId, chunkData);
    manager.writePiece(piece);
    final File staged = new File(tempDir, piece.getPieceRefs().get(0).getRelativePath());
    assertTrue(manager.prepare(prepareNode(loadId), Collections.emptyMap()));

    final LoadTsFileConsensusNode commit = commitNode(loadId);
    commit.setSearchIndex(10L);
    assertTrue(manager.loadAll(commit, Collections.emptyMap()));

    Mockito.verify(dataRegion)
        .loadNewTsFile(
            Mockito.any(),
            Mockito.eq(true),
            Mockito.anyBoolean(),
            Mockito.anyBoolean(),
            Mockito.<Optional<Map<String, Long>>>any());
    assertFalse(staged.exists());
    assertFalse(staged.getParentFile().exists());
  }

  /**
   * Stubs the WAL node of the region with one whose safe-deletion watermark this test controls. A
   * real WAL node is not usable here: it only completes the flush listener of an entry once its
   * sync task has run, which needs a DataNode around it.
   */
  private void useWatermarkWALNode(final long safelyDeletedSearchIndex) {
    final IWALNode walNode = Mockito.mock(IWALNode.class);
    walWatermark.set(safelyDeletedSearchIndex);
    Mockito.when(walNode.getSafelyDeletedSearchIndex())
        .thenAnswer(invocation -> walWatermark.get());
    Mockito.doAnswer(
            invocation -> {
              walWatermarkListener.set(invocation.getArgument(0));
              return null;
            })
        .when(walNode)
        .setSafeDeletedSearchIndexListener(Mockito.any());
    final WALFlushListener flushListener = Mockito.mock(WALFlushListener.class);
    Mockito.when(flushListener.waitForResult()).thenReturn(WALFlushListener.Status.SUCCESS);
    Mockito.when(walNode.log(Mockito.anyLong(), Mockito.any(LoadTsFileConsensusNode.class)))
        .thenReturn(flushListener);
    Mockito.when(dataRegion.getWALNode()).thenReturn(Optional.of(walNode));
  }

  /**
   * Advances the watermark the way the consensus layer does: the new value becomes readable and the
   * release listener the retention registered for it is notified.
   */
  private void advanceWatermark(final long safelyDeletedSearchIndex) {
    walWatermark.set(safelyDeletedSearchIndex);
    final LongConsumer listener = walWatermarkListener.get();
    if (listener != null) {
      listener.accept(safelyDeletedSearchIndex);
    }
  }

  /**
   * Makes the mocked DataRegion import a staged file the way the real one does: it moves the file
   * and its resource into the data directory when the caller lets it take the staged file away, and
   * copies both of them otherwise.
   */
  private void emulateStagedFileImport() throws Exception {
    final File dataDir = new File(tempDir, "data");
    Mockito.doAnswer(
            invocation -> {
              final TsFileResource resource = invocation.getArgument(0);
              final boolean deleteOriginFile = invocation.getArgument(1);
              final File staged = resource.getTsFile();
              final File target = new File(dataDir, staged.getName());
              assertTrue(target.getParentFile().mkdirs() || target.getParentFile().isDirectory());
              final File stagedResource =
                  new File(staged.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
              final File targetResource =
                  new File(target.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
              if (deleteOriginFile) {
                Files.move(staged.toPath(), target.toPath());
                if (stagedResource.isFile()) {
                  Files.move(stagedResource.toPath(), targetResource.toPath());
                }
              } else {
                Files.copy(staged.toPath(), target.toPath());
                if (stagedResource.isFile()) {
                  Files.copy(stagedResource.toPath(), targetResource.toPath());
                }
              }
              resource.setFile(target);
              return null;
            })
        .when(dataRegion)
        .loadNewTsFile(
            Mockito.any(),
            Mockito.anyBoolean(),
            Mockito.anyBoolean(),
            Mockito.anyBoolean(),
            Mockito.<Optional<Map<String, Long>>>any());
  }

  /** A PIECE that carries one chunk, which stages it and records where its payload landed. */
  private static LoadTsFileConsensusNode stagedPiece(
      final String loadId, final NonAlignedChunkData chunkData) {
    return LoadTsFileConsensusNode.piece(
        new PlanNodeId("load-piece-" + loadId),
        loadId,
        "file-1",
        0L,
        new ArrayList<>(Collections.singletonList(chunkData)));
  }

  private static LoadTsFileConsensusNode commitNode(final String loadId) {
    return LoadTsFileConsensusNode.commit(
        new PlanNodeId("load-commit-" + loadId),
        loadId,
        "file-1",
        false,
        false,
        Collections.emptyMap());
  }

  private static byte[] toByteArray(final ByteBuffer buffer) {
    final byte[] result = new byte[buffer.remaining()];
    buffer.duplicate().get(result);
    return result;
  }

  /** The ABORT command of a task, which is what discards the staged files of a task. */
  private static LoadTsFileConsensusNode abortNode(final String loadId) {
    return LoadTsFileConsensusNode.abort(
        new PlanNodeId("load-abort-" + loadId), loadId, null, false);
  }

  /** The PREPARE command of a task, which is what seals the staged files. */
  private static LoadTsFileConsensusNode prepareNode(final String loadId) {
    return LoadTsFileConsensusNode.prepare(
        new PlanNodeId("load-prepare-" + loadId),
        loadId,
        null,
        0,
        0L,
        0L,
        false,
        Collections.emptyMap());
  }

  private static NonAlignedChunkData laidOutChunk() {
    final NonAlignedChunkData chunkData =
        createNonAlignedChunkData(
            new StringArrayDeviceID("root", "load_manager_test", "d0"), "s0", 0, 10);
    // The staged writer only accepts chunks whose layout was assigned before writing.
    new ChunkOffsetCalculator().assign(chunkData);
    return chunkData;
  }

  private static NonAlignedChunkData createNonAlignedChunkData(
      final IDeviceID device, final String measurement, final int valueBase, final int pointCount) {
    final NonAlignedChunkData chunkData =
        (NonAlignedChunkData)
            ChunkData.createChunkData(
                false,
                device,
                new ChunkHeader(
                    measurement,
                    0,
                    TSDataType.INT32,
                    CompressionType.UNCOMPRESSED,
                    TSEncoding.PLAIN,
                    0),
                new TTimePartitionSlot(0L));
    final long[] times = new long[pointCount];
    final Object[] values = new Object[pointCount];
    for (int i = 0; i < pointCount; i++) {
      times[i] = i + 1L;
      values[i] = valueBase + i;
    }
    chunkData.writeDecodePage(times, values, pointCount);
    chunkData.endChunk();
    return chunkData;
  }

  private static List<TsFileData> toTsFileDataList(final List<ChunkData> chunkDataList) {
    final List<TsFileData> result = new ArrayList<>();
    for (final ChunkData chunkData : chunkDataList) {
      result.add(chunkData);
    }
    return result;
  }

  private static void assertAllSeriesReadable(
      final File tsFile, final List<SeriesExpectation> expectations) throws Exception {
    try (final TsFileReader reader = new TsFileReader(tsFile)) {
      for (final SeriesExpectation expectation : expectations) {
        final QueryExpression queryExpression =
            QueryExpression.create(
                Collections.singletonList(
                    new Path(expectation.device, expectation.measurement, false)),
                null);
        final QueryDataSet dataSet = reader.query(queryExpression);
        int count = 0;
        while (dataSet.hasNext()) {
          final RowRecord row = dataSet.next();
          final org.apache.tsfile.read.common.Field field = row.getField(0);
          final int value = field.getIntV();
          if (count == 0) {
            assertEquals(expectation.firstValue, value);
          }
          if (count == expectation.pointCount - 1) {
            assertEquals(expectation.lastValue, value);
          }
          count++;
        }
        assertEquals(expectation.pointCount, count);
      }
    }
  }

  private static String hex(final byte[] bytes, final int from, final int length) {
    final StringBuilder builder = new StringBuilder();
    for (int i = from; i < Math.min(from + length, bytes.length); i++) {
      builder.append(String.format("%02x", bytes[i]));
    }
    return builder.toString();
  }

  private static void deleteRecursively(final File file) {
    if (file == null || !file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      final File[] files = file.listFiles();
      if (files != null) {
        for (final File child : files) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }

  private static final class SeriesExpectation {
    private final IDeviceID device;
    private final String measurement;
    private final int pointCount;
    private final int firstValue;
    private final int lastValue;

    private SeriesExpectation(
        final IDeviceID device,
        final String measurement,
        final int pointCount,
        final int firstValue,
        final int lastValue) {
      this.device = device;
      this.measurement = measurement;
      this.pointCount = pointCount;
      this.firstValue = firstValue;
      this.lastValue = lastValue;
    }
  }
}
