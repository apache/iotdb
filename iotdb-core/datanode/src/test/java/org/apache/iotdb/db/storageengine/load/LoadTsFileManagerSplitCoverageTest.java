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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadPieceConsensusRequest;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.ChunkOffsetCalculator;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkPayloadRef;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkPayloadUnavailableException;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileSplitter;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end coverage for the LOAD splitter + {@link LoadTsFileManager} write path.
 *
 * <p>Each scenario composes one source TsFile out of many chunk groups, mixing aligned and
 * non-aligned devices, several measurements per device (including measurements that are entirely
 * null), several chunks per series, several pages per chunk, page and chunk ranges that cross time
 * partition boundaries, and layouts where the chunks of one measurement are not adjacent. The file
 * is then driven through {@link TsFileSplitter} (including the decode/re-encode path that exercises
 * {@code sealCurrentPage} at page and time-partition boundaries), every piece is assigned a
 * pre-calculated absolute offset by {@link ChunkOffsetCalculator}, the pieces are handed to {@link
 * LoadTsFileManager#writePiece} in a non-source order, and finally every series is read back from
 * the staged TsFiles and compared point by point with what was written.
 *
 * <p>Offsets are pre-calculated per time partition, exactly as {@code PieceDispatcher} does it, so
 * each staged TsFile sees offsets relative to its own start. A piece holds the chunks of one time
 * partition, and a partition may be dispatched as several pieces which may arrive in any order.
 *
 * <p>The scenarios are sized so that the write path is exercised with a non-trivial amount of data
 * (see {@link #testBulkVolume}, which stages several hundred thousand points) and with many
 * combinations per TsFile rather than one device at a time.
 */
public class LoadTsFileManagerSplitCoverageTest {

  private static final int SOURCE_PAGE_SIZE = 64 * 1024 * 1024;
  private static final int SOURCE_MAX_POINTS_PER_PAGE = 1_000_000;

  /** Re-encode during splitting with a tiny page budget to force page boundaries in that path. */
  private static final int SPLIT_PAGE_SIZE = 64;

  private static final int SPLIT_MAX_POINTS_PER_PAGE = 10;

  /**
   * LOAD refuses to split a TsFile that spans more time partitions than this. The coverage matrix
   * deliberately spans many partitions, so the limit is raised for the duration of the test.
   */
  private static final int SPLIT_PARTITION_MAX_SIZE = 1_000_000;

  private static final long FINE_PARTITION_INTERVAL = 100L;
  private static final long COARSE_PARTITION_INTERVAL = 1_000L;
  private static final long BULK_PARTITION_INTERVAL = 100_000L;

  /** Scenarios up to this size also verify the source TsFile before it is split. */
  private static final long SOURCE_SELF_CHECK_MAX_POINTS = 100_000L;

  private File tempDir;
  private String[] originalLoadBaseDirs;
  private DataRegion dataRegion;
  private IoTDBConfig config;
  private int originalPageSize;
  private int originalMaxPointsInPage;
  private int originalSplitPartitionMaxSize;
  private long originalTimePartitionOrigin;
  private long originalTimePartitionInterval;

  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("load-split-coverage-test").toFile();
    config = IoTDBDescriptor.getInstance().getConfig();
    originalLoadBaseDirs = config.getLoadTsFileDirs();
    config.setLoadTsFileDirs(new String[] {tempDir.getAbsolutePath()});
    originalSplitPartitionMaxSize = config.getLoadTsFileSpiltPartitionMaxSize();
    config.setLoadTsFileSpiltPartitionMaxSize(SPLIT_PARTITION_MAX_SIZE);

    originalPageSize = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    originalMaxPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

    originalTimePartitionOrigin =
        CommonDescriptor.getInstance().getConfig().getTimePartitionOrigin();
    originalTimePartitionInterval =
        CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();
    CommonDescriptor.getInstance().getConfig().setTimePartitionOrigin(0L);
    TimePartitionUtils.setTimePartitionOrigin(0L);

    dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn("root.split_coverage");
    Mockito.when(dataRegion.getDataRegionIdString()).thenReturn("0");
    Mockito.when(dataRegion.getNonSystemDatabaseName())
        .thenReturn(Optional.of("root.split_coverage"));
  }

  @After
  public void tearDown() throws Exception {
    final TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    tsFileConfig.setPageSizeInByte(originalPageSize);
    tsFileConfig.setMaxNumberOfPointsInPage(originalMaxPointsInPage);
    CommonDescriptor.getInstance().getConfig().setTimePartitionOrigin(originalTimePartitionOrigin);
    CommonDescriptor.getInstance()
        .getConfig()
        .setTimePartitionInterval(originalTimePartitionInterval);
    TimePartitionUtils.setTimePartitionOrigin(originalTimePartitionOrigin);
    TimePartitionUtils.setTimePartitionInterval(originalTimePartitionInterval);
    config.setLoadTsFileDirs(originalLoadBaseDirs);
    config.setLoadTsFileSpiltPartitionMaxSize(originalSplitPartitionMaxSize);
    deleteRecursively(tempDir);
  }

  // ------------------------------------------------------------------
  // Scenarios
  // ------------------------------------------------------------------

  /**
   * Every supported data type appears twice in the same TsFile, once as an aligned device and once
   * as a non-aligned device, each carrying three measurements with different null patterns.
   */
  @Test
  public void testAllDataTypesMixedInOneTsFile() throws Exception {
    final List<TSDataType> types =
        Arrays.asList(
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.BOOLEAN,
            TSDataType.TEXT,
            TSDataType.STRING,
            TSDataType.BLOB);
    final List<DeviceSpec> devices = new ArrayList<>();
    for (final TSDataType type : types) {
      final List<long[][]> shape = richShape(0L);
      devices.add(
          alignedDevice(
              "type_" + type + "_aligned",
              ChunkOrder.CHUNK_MAJOR,
              series("m0", type, NullMode.NONE, shape),
              series("m1", type, NullMode.ALTERNATE, shape),
              series("m2", type, NullMode.ALL, shape)));
      devices.add(
          nonAlignedDevice(
              "type_" + type + "_plain",
              ChunkOrder.SERIES_MAJOR,
              series("m0", type, NullMode.NONE, shape),
              series("m1", type, NullMode.ALTERNATE, shape),
              series("m2", type, NullMode.SPARSE, shape)));
    }
    runScenario(
        new Scenario(
            "all-types-mixed",
            COARSE_PARTITION_INTERVAL,
            devices,
            new DispatchPlan(2, false, true),
            12_000L));
  }

  /** Chunk/page shapes that cross one or more time partition boundaries. */
  @Test
  public void testTimePartitionBoundaryShapes() throws Exception {
    final List<DeviceSpec> devices = new ArrayList<>();
    devices.addAll(boundaryPair("spread", fineSpreadShape(0L), fineSpreadShape(0L)));
    devices.addAll(
        boundaryPair("chunk_crossing", fineChunkCrossingShape(400L), fineChunkCrossingShape(400L)));
    devices.addAll(
        boundaryPair("page_crossing", finePageCrossingShape(800L), finePageCrossingShape(800L)));
    devices.addAll(boundaryPair("mixed", fineMixedShape(1_200L), fineMixedShape(1_200L)));
    runScenario(
        new Scenario(
            "boundary-shapes",
            FINE_PARTITION_INTERVAL,
            devices,
            new DispatchPlan(1, true, false),
            1_000L));
  }

  /**
   * Aligned devices with 1..5 measurements, several null patterns, several chunks per series and a
   * device whose chunks hold up to ten pages of mixed sizes.
   */
  @Test
  public void testAlignedMeasurementMatrix() throws Exception {
    final List<DeviceSpec> devices = new ArrayList<>();
    devices.add(
        alignedDevice(
            "aligned_one_measurement",
            ChunkOrder.CHUNK_MAJOR,
            series("m0", TSDataType.INT64, NullMode.NONE, richShape(0L))));
    devices.add(
        alignedDevice(
            "aligned_two_measurements",
            ChunkOrder.CHUNK_MAJOR,
            series("m0", TSDataType.INT32, NullMode.NONE, richShape(0L)),
            series("m1", TSDataType.DOUBLE, NullMode.ALTERNATE, richShape(0L))));
    devices.add(
        alignedDevice(
            "aligned_three_measurements",
            ChunkOrder.CHUNK_MAJOR,
            series("m0", TSDataType.INT64, NullMode.NONE, richShape(0L)),
            series("m1", TSDataType.FLOAT, NullMode.SPARSE, richShape(0L)),
            series("m2", TSDataType.BOOLEAN, NullMode.ALTERNATE_CHUNK, richShape(0L))));
    devices.add(
        alignedDevice(
            "aligned_five_measurements",
            ChunkOrder.CHUNK_MAJOR,
            series("m0", TSDataType.INT32, NullMode.NONE, richShape(0L)),
            series("m1", TSDataType.INT64, NullMode.ALTERNATE, richShape(0L)),
            series("m2", TSDataType.DOUBLE, NullMode.SPARSE, richShape(0L)),
            series("m3", TSDataType.TEXT, NullMode.ALL, richShape(0L)),
            series("m4", TSDataType.BLOB, NullMode.ALTERNATE_CHUNK, richShape(0L))));
    devices.add(
        alignedDevice(
            "aligned_five_measurements_multi_page",
            ChunkOrder.CHUNK_MAJOR,
            series("m0", TSDataType.DOUBLE, NullMode.NONE, manyPageShape(0L)),
            series("m1", TSDataType.INT32, NullMode.ALTERNATE, manyPageShape(0L)),
            series("m2", TSDataType.INT64, NullMode.NONE, manyPageShape(0L)),
            series("m3", TSDataType.FLOAT, NullMode.SPARSE, manyPageShape(0L)),
            series("m4", TSDataType.STRING, NullMode.ALTERNATE, manyPageShape(0L))));
    runScenario(
        new Scenario(
            "aligned-matrix",
            COARSE_PARTITION_INTERVAL,
            devices,
            new DispatchPlan(3, false, false),
            5_000L));
  }

  /** Non-aligned devices with 1..5 measurements, both chunk layouts and null-heavy series. */
  @Test
  public void testNonAlignedMeasurementMatrix() throws Exception {
    final List<DeviceSpec> devices = new ArrayList<>();
    devices.add(
        nonAlignedDevice(
            "plain_one_measurement",
            ChunkOrder.SERIES_MAJOR,
            series("m0", TSDataType.INT32, NullMode.NONE, richShape(0L))));
    devices.add(
        nonAlignedDevice(
            "plain_two_measurements",
            ChunkOrder.CHUNK_MAJOR,
            series("m0", TSDataType.TEXT, NullMode.NONE, richShape(0L)),
            series("m1", TSDataType.INT64, NullMode.ALTERNATE, richShape(0L))));
    devices.add(
        nonAlignedDevice(
            "plain_three_measurements",
            ChunkOrder.SERIES_MAJOR,
            series("m0", TSDataType.DOUBLE, NullMode.NONE, richShape(0L)),
            series("m1", TSDataType.BLOB, NullMode.SPARSE, richShape(0L)),
            series("m2", TSDataType.BOOLEAN, NullMode.ALTERNATE_CHUNK, richShape(0L))));
    devices.add(
        nonAlignedDevice(
            "plain_five_measurements",
            ChunkOrder.CHUNK_MAJOR,
            series("m0", TSDataType.INT64, NullMode.ALTERNATE, richShape(0L)),
            series("m1", TSDataType.INT32, NullMode.NONE, richShape(0L)),
            series("m2", TSDataType.FLOAT, NullMode.SPARSE, richShape(0L)),
            series("m3", TSDataType.STRING, NullMode.ALTERNATE_CHUNK, richShape(0L)),
            series("m4", TSDataType.DOUBLE, NullMode.NONE, richShape(0L))));
    runScenario(
        new Scenario(
            "plain-matrix",
            COARSE_PARTITION_INTERVAL,
            devices,
            new DispatchPlan(2, true, true),
            4_000L));
  }

  /**
   * A volume scenario: 8 devices (4 aligned, 4 non-aligned) with 4 measurements each and 25_000
   * points per series, that is 800_000 points staged through the splitter and the pre-calculated
   * chunk writer.
   */
  @Test
  public void testBulkVolume() throws Exception {
    final List<DeviceSpec> devices = new ArrayList<>();
    final TSDataType[] types = {
      TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE
    };
    for (int i = 0; i < types.length; i++) {
      devices.add(
          alignedDevice(
              "bulk_aligned_" + i,
              ChunkOrder.CHUNK_MAJOR,
              series("m0", types[i], NullMode.NONE, bulkShape()),
              series("m1", types[i], NullMode.NONE, bulkShape()),
              series("m2", types[i], NullMode.ALTERNATE, bulkShape()),
              series("m3", types[(i + 1) % types.length], NullMode.NONE, bulkShape())));
      devices.add(
          nonAlignedDevice(
              "bulk_plain_" + i,
              ChunkOrder.SERIES_MAJOR,
              series("m0", types[i], NullMode.NONE, bulkShape()),
              series("m1", types[(i + 2) % types.length], NullMode.NONE, bulkShape()),
              series("m2", types[i], NullMode.SPARSE, bulkShape()),
              series("m3", types[(i + 3) % types.length], NullMode.NONE, bulkShape())));
    }
    runScenario(
        new Scenario(
            "bulk-volume",
            BULK_PARTITION_INTERVAL,
            devices,
            new DispatchPlan(2, true, true),
            600_000L));
  }

  /** The same composition dispatched as different numbers of pieces in different orders. */
  @Test
  public void testPieceDispatchOrders() throws Exception {
    final List<DispatchPlan> plans =
        Arrays.asList(
            new DispatchPlan(1, false, false),
            new DispatchPlan(1, true, false),
            new DispatchPlan(2, false, false),
            new DispatchPlan(3, true, true),
            new DispatchPlan(0, true, true));
    for (final DispatchPlan plan : plans) {
      final List<DeviceSpec> devices = new ArrayList<>();
      devices.add(
          alignedDevice(
              "dispatch_aligned",
              ChunkOrder.CHUNK_MAJOR,
              series("m0", TSDataType.INT64, NullMode.NONE, richShape(0L)),
              series("m1", TSDataType.DOUBLE, NullMode.ALTERNATE, richShape(0L))));
      devices.add(
          nonAlignedDevice(
              "dispatch_plain",
              ChunkOrder.SERIES_MAJOR,
              series("m0", TSDataType.INT32, NullMode.NONE, richShape(0L)),
              series("m1", TSDataType.TEXT, NullMode.SPARSE, richShape(0L))));
      runScenario(
          new Scenario("dispatch-" + plan, COARSE_PARTITION_INTERVAL, devices, plan, 1_000L));
    }
  }

  /**
   * Many devices of both kinds spread over many time partitions: twelve devices with different
   * shapes and both non-aligned chunk layouts, split at a fine granularity so that every staged
   * TsFile holds several devices while every device spans several files, dispatched as four pieces
   * per partition in reverse order.
   */
  @Test
  public void testManyDevicesManyPartitions() throws Exception {
    final List<DeviceSpec> devices = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      final long base = i * 300L;
      devices.add(
          alignedDevice(
              "many_aligned_" + i,
              ChunkOrder.CHUNK_MAJOR,
              series("m0", TSDataType.INT64, NullMode.NONE, fineMixedShape(base)),
              series("m1", TSDataType.DOUBLE, NullMode.ALTERNATE, fineMixedShape(base)),
              series("m2", TSDataType.TEXT, NullMode.SPARSE, fineMixedShape(base))));
      devices.add(
          nonAlignedDevice(
              "many_plain_" + i,
              i % 2 == 0 ? ChunkOrder.SERIES_MAJOR : ChunkOrder.CHUNK_MAJOR,
              series("m0", TSDataType.INT32, NullMode.NONE, fineChunkCrossingShape(base)),
              series(
                  "m1",
                  TSDataType.BOOLEAN,
                  NullMode.ALTERNATE_CHUNK,
                  fineChunkCrossingShape(base))));
    }
    runScenario(
        new Scenario(
            "many-devices-many-partitions",
            FINE_PARTITION_INTERVAL,
            devices,
            new DispatchPlan(4, true, true),
            700L));
  }

  /**
   * The leader stages a piece, logs it to the WAL with the metadata of every chunk plus a reference
   * to the payload of each of them, and reads those references back when it forwards the piece.
   *
   * <p>A replica therefore receives exactly the piece the leader received, and a piece restored
   * from the WAL entry alone expands to the same bytes; if the staged payload is gone, the
   * expansion is refused instead of forwarding a piece whose chunks would be imported empty.
   */
  @Test
  public void testPieceLoggedToWalExpandsToThePieceTheLeaderReceived() throws Exception {
    setTimePartitionInterval(COARSE_PARTITION_INTERVAL);
    final File sourceTsFile = new File(tempDir, "wal-piece-source.tsfile");
    setPageConfig(SOURCE_PAGE_SIZE, SOURCE_MAX_POINTS_PER_PAGE);
    writeSourceTsFile(
        sourceTsFile,
        Collections.singletonList(
            alignedDevice(
                "wal_aligned",
                ChunkOrder.CHUNK_MAJOR,
                series("m0", TSDataType.INT64, NullMode.NONE, richShape(0L)))));
    setPageConfig(SPLIT_PAGE_SIZE, SPLIT_MAX_POINTS_PER_PAGE);

    final Map<Long, List<ChunkData>> partition2Chunks = groupByPartition(split(sourceTsFile));
    final List<ChunkData> chunks = partition2Chunks.values().iterator().next();
    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    for (final ChunkData chunkData : chunks) {
      calculator.assign(chunkData);
    }
    final byte[] payload = firstChunkPayload(chunks.get(0));

    final String uuid = "wal-piece";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    try {
      final LoadTsFileConsensusNode node =
          LoadTsFileConsensusNode.piece(
              new PlanNodeId("wal"), uuid, "source.tsfile", 0L, new ArrayList<>(chunks));
      manager.writePiece(node);

      assertFalse(
          "staging a piece must record the files it touched", node.getPieceRefs().isEmpty());
      for (final TsFileData data : node.getTsFileDataList()) {
        assertFalse(
            "every staged chunk must record where its payload landed",
            ((ChunkData) data).getChunkPayloadRefs().isEmpty());
      }

      // The forwarded form carries the payload, the logged form only references it
      final byte[] forwarded = toByteArray(node.serialize());
      final byte[] logged = toByteArray(node.serialize(false));
      assertTrue(forwarded.length > logged.length);
      assertTrue(contains(forwarded, payload));
      assertFalse("the WAL must not store the staged payload twice", contains(logged, payload));

      // A piece restored from the logged form alone expands to exactly the forwarded piece. The
      // logged form is the envelope the dispatcher writes (entry type, sentinel mem-table id, node
      // type) followed by the node itself.
      final LoadTsFileConsensusNode restored =
          LoadTsFileConsensusNode.deserializeFromWAL(
              ByteBuffer.wrap(
                      logged, DISPATCH_ENVELOPE_BYTES, logged.length - DISPATCH_ENVELOPE_BYTES)
                  .slice());
      assertEquals(node.getDataSize(), restored.getDataSize());
      assertEquals(node.getSearchIndex(), restored.getSearchIndex());
      assertEquals(node.getPieceRefs().get(0).getSize(), restored.getPieceRefs().get(0).getSize());
      assertEquals(
          chunks.get(0).getChunkLayout(),
          ((ChunkData) restored.getTsFileDataList().get(0)).getChunkLayout());
      assertEquals(
          chunks.get(0).getChunks().get(0).getChunkStatistic().getStartTime(),
          ((ChunkData) restored.getTsFileDataList().get(0))
              .getChunks()
              .get(0)
              .getChunkStatistic()
              .getStartTime());
      assertArrayEquals(
          "the follower must receive the very same piece the leader received",
          forwarded,
          toByteArray(restored.serialize()));

      // The replication request of a replica keeps only the metadata and the payload references
      // while it waits in the queues, and reads the payloads back when it is actually sent
      final LoadPieceConsensusRequest request = new LoadPieceConsensusRequest(restored);
      assertTrue(
          "the replication request must defer its serialization",
          request.isSerializationDeferred());
      assertEquals(
          "a queued request must retain the reference form and nothing more",
          toByteArray(restored.serialize(false)).length,
          request.getMemorySize());
      assertTrue(request.getMemorySize() < forwarded.length);
      assertTrue(request.getSerializedSize() > request.getMemorySize());
      assertArrayEquals(
          "sending must expand to the very same piece the leader received",
          forwarded,
          toByteArray(request.serializeToByteBuffer()));

      // Without the staged payload the piece must be refused rather than imported empty
      final File staged = new File(restored.getPieceRefs().get(0).getRelativePath());
      assertTrue(staged.delete());
      try {
        restored.serialize();
        fail("expected the expansion of a cleaned up piece to fail");
      } catch (final ChunkPayloadUnavailableException e) {
        assertTrue(e.getMessage().contains(staged.getName()));
      }
      // The dispatcher still has the reference-only form to fall back to
      assertTrue(toByteArray(restored.serialize(false)).length > 0);
      // and a request that can no longer expand degrades to it instead of killing the pipeline
      assertFalse(contains(toByteArray(request.serializeToByteBuffer()), payload));

      // A reference that points outside of the staging directories must never be followed
      final File outside = new File(tempDir.getParentFile(), "outside-staging.tsfile");
      Files.write(outside.toPath(), new byte[] {1, 2, 3, 4});
      try {
        final ChunkData chunkData = (ChunkData) restored.getTsFileDataList().get(0);
        chunkData.setChunkPayloadRefs(
            Collections.singletonList(new ChunkPayloadRef(outside.getAbsolutePath(), 0L, 4L)));
        restored.serialize();
        fail("expected a reference outside of the staging directories to be rejected");
      } catch (final ChunkPayloadUnavailableException e) {
        assertTrue(e.getMessage().contains("outside-staging.tsfile"));
      }
    } finally {
      manager.deleteAll(abortNode(uuid));
    }
  }

  /**
   * A staged file of a previous run is resumed from what is on disk: the chunk ranges recorded in
   * its progress file are read back, the writer is rebuilt at the end of the gap-free prefix and it
   * still seals a TsFile whose chunks are reachable.
   *
   * <p>The records live in the progress file only, so a recovery that forgets to read them back
   * concludes that nothing on disk is complete and silently drops the whole task.
   */
  @Test
  public void testRecoverFromDiskResumesStagedFileOfPreviousRun() throws Exception {
    setTimePartitionInterval(COARSE_PARTITION_INTERVAL);
    final File sourceTsFile = new File(tempDir, "recover-source.tsfile");
    setPageConfig(SOURCE_PAGE_SIZE, SOURCE_MAX_POINTS_PER_PAGE);
    writeSourceTsFile(
        sourceTsFile,
        Collections.singletonList(
            nonAlignedDevice(
                "recover_device",
                ChunkOrder.CHUNK_MAJOR,
                series("m0", TSDataType.INT64, NullMode.NONE, richShape(0L)))));
    setPageConfig(SPLIT_PAGE_SIZE, SPLIT_MAX_POINTS_PER_PAGE);

    final Map<Long, List<ChunkData>> partition2Chunks = groupByPartition(split(sourceTsFile));
    for (final List<ChunkData> partition : partition2Chunks.values()) {
      final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
      for (final ChunkData chunkData : partition) {
        calculator.assign(chunkData);
      }
    }

    final String uuid = "recover-task";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    try {
      for (final List<TsFileData> piece :
          buildPieces(partition2Chunks, new DispatchPlan(1, false, false))) {
        manager.writePiece(uuid, piece);
      }
      final Set<String> stagedNames = stagedFileNames(uuid);
      assertFalse("the scenario staged no file", stagedNames.isEmpty());

      // The restart: nothing of the previous run is in memory any more, only the staged files and
      // the progress files next to them.
      final File taskDir = findDirectory(tempDir, uuid);
      assertNotNull(taskDir);
      final TsFileWriterManager recovered =
          new TsFileWriterManager(dataRegion, taskDir).recoverFromDisk();

      assertTrue(
          "the chunk ranges of the staged files must be read back, otherwise the task looks "
              + "unresumable and its staged data is dropped",
          recovered.hasStagedData());
      assertEquals(
          "every staged file of the task must be resumed",
          stagedNames.size(),
          countStagedFiles(taskDir));

      // The resumed writers have to seal files that can still be read, which is only possible if
      // the chunk metadata was restored from the records as well.
      recovered.prepare(false, Collections.emptyMap());
      for (final File staged : stagedFiles(taskDir)) {
        try (final TsFileSequenceReader reader =
            new TsFileSequenceReader(staged.getAbsolutePath())) {
          assertEquals(
              "the resumed writer must seal the chunks it found on disk",
              1,
              reader.getAllMeasurements().size());
        }
      }
    } finally {
      manager.deleteAll(abortNode(uuid));
    }
  }

  private static int countStagedFiles(final File taskDir) {
    return stagedFiles(taskDir).size();
  }

  private static List<File> stagedFiles(final File taskDir) {
    final List<File> result = new ArrayList<>();
    final File[] files = taskDir.listFiles();
    if (files != null) {
      for (final File file : files) {
        if (file.isFile() && file.getName().endsWith(".tsfile")) {
          result.add(file);
        }
      }
    }
    Collections.sort(result);
    return result;
  }

  /**
   * A piece that reaches the same region twice stays harmless: the retry of a request whose
   * response was lost, and the replay of the WAL entry of that piece, arrive with the payload
   * references the first write recorded instead of the payload itself. Writing them again would
   * repeat the chunks, so the repetition is skipped, while a piece that cannot be explained that
   * way is still refused.
   */
  @Test
  public void testPieceAppliedTwiceIsWrittenOnce() throws Exception {
    setTimePartitionInterval(COARSE_PARTITION_INTERVAL);
    final File sourceTsFile = new File(tempDir, "dedup-source.tsfile");
    setPageConfig(SOURCE_PAGE_SIZE, SOURCE_MAX_POINTS_PER_PAGE);
    writeSourceTsFile(
        sourceTsFile,
        Collections.singletonList(
            nonAlignedDevice(
                "dedup_device",
                ChunkOrder.CHUNK_MAJOR,
                series("m0", TSDataType.INT64, NullMode.NONE, richShape(0L)))));
    setPageConfig(SPLIT_PAGE_SIZE, SPLIT_MAX_POINTS_PER_PAGE);

    final Map<Long, List<ChunkData>> partition2Chunks = groupByPartition(split(sourceTsFile));
    for (final List<ChunkData> partition : partition2Chunks.values()) {
      final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
      for (final ChunkData chunkData : partition) {
        calculator.assign(chunkData);
      }
    }
    final List<TsFileData> pieceData =
        buildPieces(partition2Chunks, new DispatchPlan(1, false, false)).get(0);

    final String uuid = "dedup-task";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    try {
      final LoadTsFileConsensusNode piece =
          LoadTsFileConsensusNode.piece(
              new PlanNodeId("dedup-piece"),
              uuid,
              sourceTsFile.getName(),
              0L,
              new ArrayList<>(pieceData));
      final List<LoadTsFileConsensusNode.PieceRef> firstWrite = manager.writePiece(piece);
      assertFalse("the piece must stage something", firstWrite.isEmpty());
      final File staged = new File(firstWrite.get(0).getRelativePath());
      final long lengthAfterFirstWrite = staged.length();
      final int recordedRangesAfterFirstWrite =
          new LoadTsFileProgress(staged).readAllRecords().size();
      assertTrue(recordedRangesAfterFirstWrite > 0);

      // The very same piece arrives again, as the retry of a request whose response was lost.
      manager.writePiece(piece);
      assertEquals(
          "a repeated piece must not append its chunks again",
          lengthAfterFirstWrite,
          staged.length());
      assertEquals(
          "a repeated piece must not record its chunk ranges again",
          recordedRangesAfterFirstWrite,
          new LoadTsFileProgress(staged).readAllRecords().size());

      // A WAL entry of the piece is replayed after a restart: the staged file keeps its content.
      final File taskDir = findDirectory(tempDir, uuid);
      assertNotNull(taskDir);
      new TsFileWriterManager(dataRegion, taskDir).recoverFromDisk();
      final LoadTsFileConsensusNode replayed =
          LoadTsFileConsensusNode.piece(
              new PlanNodeId("dedup-piece"),
              uuid,
              sourceTsFile.getName(),
              0L,
              new ArrayList<>(pieceData));
      manager.writePiece(replayed);
      assertEquals(
          "a replayed piece must not append its chunks again",
          lengthAfterFirstWrite,
          staged.length());
      assertEquals(
          recordedRangesAfterFirstWrite, new LoadTsFileProgress(staged).readAllRecords().size());
    } finally {
      manager.deleteAll(abortNode(uuid));
    }
  }

  /**
   * The same piece built a second time - the bytes still in memory, which is what a replica that
   * receives it twice gets, or what a leader re-dispatches - is recognized from the offsets the
   * staged file already records, so no chunk is appended a second time.
   */
  @Test
  public void testIdenticalPieceWithPayloadIsNotWrittenTwice() throws Exception {
    setTimePartitionInterval(COARSE_PARTITION_INTERVAL);
    final File sourceTsFile = new File(tempDir, "dedup-payload-source.tsfile");
    setPageConfig(SOURCE_PAGE_SIZE, SOURCE_MAX_POINTS_PER_PAGE);
    writeSourceTsFile(
        sourceTsFile,
        Collections.singletonList(
            nonAlignedDevice(
                "dedup_payload_device",
                ChunkOrder.CHUNK_MAJOR,
                series("m0", TSDataType.INT64, NullMode.NONE, richShape(0L)))));
    setPageConfig(SPLIT_PAGE_SIZE, SPLIT_MAX_POINTS_PER_PAGE);

    final String uuid = "dedup-payload-task";
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    try {
      final List<TsFileData> firstPiece = singlePieceData(sourceTsFile);
      final List<LoadTsFileConsensusNode.PieceRef> refs =
          manager.writePiece(
              LoadTsFileConsensusNode.piece(
                  new PlanNodeId("dedup-payload-piece"),
                  uuid,
                  sourceTsFile.getName(),
                  0L,
                  firstPiece));
      assertFalse(refs.isEmpty());
      final File staged = new File(refs.get(0).getRelativePath());
      final long lengthAfterFirstWrite = staged.length();
      final int rangesAfterFirstWrite = new LoadTsFileProgress(staged).readAllRecords().size();
      assertTrue(rangesAfterFirstWrite > 0);

      // The identical piece is built again from a fresh split, so it carries its payload: it names
      // the same offsets and must find them already staged.
      manager.writePiece(
          LoadTsFileConsensusNode.piece(
              new PlanNodeId("dedup-payload-piece"),
              uuid,
              sourceTsFile.getName(),
              0L,
              singlePieceData(sourceTsFile)));

      assertEquals(
          "an identical piece must not append its chunks again",
          lengthAfterFirstWrite,
          staged.length());
      assertEquals(
          "an identical piece must not record its chunk ranges again",
          rangesAfterFirstWrite,
          new LoadTsFileProgress(staged).readAllRecords().size());
    } finally {
      manager.deleteAll(abortNode(uuid));
    }
  }

  /** Splits the source file into the chunks and layouts of a single piece. */
  private List<TsFileData> singlePieceData(final File sourceTsFile) throws Exception {
    final Map<Long, List<ChunkData>> partition2Chunks = groupByPartition(split(sourceTsFile));
    for (final List<ChunkData> partition : partition2Chunks.values()) {
      final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
      for (final ChunkData chunkData : partition) {
        calculator.assign(chunkData);
      }
    }
    return buildPieces(partition2Chunks, new DispatchPlan(1, false, false)).get(0);
  }

  /** Entry type (1 byte) + sentinel mem-table id (8 bytes) + node type (2 bytes). */
  private static final int DISPATCH_ENVELOPE_BYTES = 11;

  private static byte[] firstChunkPayload(final ChunkData chunkData) {
    final ByteBuffer data = chunkData.getChunks().get(0).getData().duplicate();
    final byte[] payload = new byte[data.remaining()];
    data.get(payload);
    return payload;
  }

  private static byte[] toByteArray(final ByteBuffer buffer) {
    final byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  private static boolean contains(final byte[] haystack, final byte[] needle) {
    for (int i = 0; i + needle.length <= haystack.length; i++) {
      boolean matched = true;
      for (int j = 0; j < needle.length; j++) {
        if (haystack[i + j] != needle[j]) {
          matched = false;
          break;
        }
      }
      if (matched) {
        return true;
      }
    }
    return false;
  }

  // ------------------------------------------------------------------
  // Harness
  // ------------------------------------------------------------------

  private void runScenario(final Scenario scenario) throws Exception {
    setTimePartitionInterval(scenario.partitionInterval);
    final File sourceTsFile = new File(tempDir, scenario.name + "-source.tsfile");

    // Write the source with a large page budget so page boundaries are determined only by the
    // explicit sealCurrentPage calls of the builders below, not by the small split-phase budget.
    setPageConfig(SOURCE_PAGE_SIZE, SOURCE_MAX_POINTS_PER_PAGE);
    writeSourceTsFile(sourceTsFile, scenario.devices);

    // Guard the harness itself: the source must be readable as written, otherwise a later mismatch
    // says nothing about the split/write path. Skipped for the volume scenario, where it would
    // double the read cost.
    if (scenario.minPoints <= SOURCE_SELF_CHECK_MAX_POINTS) {
      final ScenarioStats sourceStats =
          verifyScenario(scenario, Collections.singletonList(sourceTsFile));
      assertTrue(
          "source TsFile of " + scenario.name + " is not readable as written",
          sourceStats.pointCount >= scenario.minPoints);
    }

    setPageConfig(SPLIT_PAGE_SIZE, SPLIT_MAX_POINTS_PER_PAGE);
    final List<ChunkData> chunkDataList = split(sourceTsFile);
    assertFalse("scenario " + scenario.name + " produced no chunk data", chunkDataList.isEmpty());
    // Pre-calculated offsets are per time partition: every staged TsFile has its own origin,
    // exactly
    // as PieceDispatcher assigns them.
    final Map<Long, List<ChunkData>> partition2Chunks = groupByPartition(chunkDataList);
    for (final List<ChunkData> partition : partition2Chunks.values()) {
      final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
      for (final ChunkData chunkData : partition) {
        calculator.assign(chunkData);
      }
    }

    final String uuid = "split-" + scenario.name;
    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    try {
      final List<List<TsFileData>> pieces = buildPieces(partition2Chunks, scenario.dispatchPlan);
      final List<LoadTsFileConsensusNode.PieceRef> refs = new ArrayList<>();
      for (final List<TsFileData> piece : pieces) {
        refs.addAll(manager.writePiece(uuid, piece));
      }
      assertFalse("scenario " + scenario.name + " produced no piece refs", refs.isEmpty());
      assertTrue(manager.prepare(prepareNode(uuid), Collections.emptyMap()));

      final List<File> targetFiles = distinctFiles(refs);
      final Set<String> stagedFiles = stagedFileNames(uuid);
      assertEquals(
          "scenario " + scenario.name + " staged files not covered by piece refs",
          stagedFiles.size(),
          targetFiles.size());

      final ScenarioStats stats = verifyScenario(scenario, targetFiles);
      stats.pieceCount = pieces.size();
      assertTrue(
          "scenario " + scenario.name + " staged only " + stats.pointCount + " points",
          stats.pointCount >= scenario.minPoints);
      System.out.println(stats);
    } finally {
      manager.deleteAll(abortNode(uuid));
    }
  }

  private List<ChunkData> split(final File sourceTsFile) throws Exception {
    final List<ChunkData> chunkDataList = new ArrayList<>();
    final TsFileSplitter splitter =
        new TsFileSplitter(
            sourceTsFile,
            tsFileData -> {
              if (tsFileData instanceof ChunkData) {
                chunkDataList.add((ChunkData) tsFileData);
              }
              return true;
            });
    splitter.splitTsFileByDataPartition();
    return chunkDataList;
  }

  private static Map<Long, List<ChunkData>> groupByPartition(final List<ChunkData> chunkDataList) {
    final Map<Long, List<ChunkData>> partition2Chunks = new LinkedHashMap<>();
    for (final ChunkData chunkData : chunkDataList) {
      partition2Chunks
          .computeIfAbsent(
              chunkData.getTimePartitionSlot().getStartTime(), key -> new ArrayList<>())
          .add(chunkData);
    }
    return partition2Chunks;
  }

  /**
   * Builds the piece list the way {@code PieceDispatcher} does: one piece holds the chunks of one
   * time partition, partitions are dispatched in source order or in reverse, and a partition may be
   * split into several pieces that are themselves dispatched in either order.
   */
  private static List<List<TsFileData>> buildPieces(
      final Map<Long, List<ChunkData>> partition2Chunks, final DispatchPlan plan) {
    final List<Long> partitions = new ArrayList<>(partition2Chunks.keySet());
    if (plan.reversePartitions) {
      Collections.reverse(partitions);
    }
    final List<List<TsFileData>> pieces = new ArrayList<>();
    for (final Long partition : partitions) {
      final List<List<TsFileData>> partitionPieces =
          splitPartition(partition2Chunks.get(partition), plan.piecesPerPartition);
      if (plan.reversePieces) {
        Collections.reverse(partitionPieces);
      }
      pieces.addAll(partitionPieces);
    }
    return pieces;
  }

  private static List<List<TsFileData>> splitPartition(
      final List<ChunkData> chunks, final int piecesPerPartition) {
    final List<List<TsFileData>> pieces = new ArrayList<>();
    if (piecesPerPartition <= 0) {
      for (final ChunkData chunkData : chunks) {
        pieces.add(new ArrayList<>(Collections.singletonList(chunkData)));
      }
      return pieces;
    }
    final int count = Math.min(piecesPerPartition, chunks.size());
    for (int pieceIndex = 0; pieceIndex < count; pieceIndex++) {
      pieces.add(new ArrayList<>());
    }
    for (int i = 0; i < chunks.size(); i++) {
      pieces.get(Math.min(count - 1, i * count / chunks.size())).add(chunks.get(i));
    }
    return pieces;
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

  private static List<File> distinctFiles(final List<LoadTsFileConsensusNode.PieceRef> refs) {
    final Set<String> paths = new LinkedHashSet<>();
    for (final LoadTsFileConsensusNode.PieceRef ref : refs) {
      paths.add(ref.getRelativePath());
    }
    final List<File> files = new ArrayList<>();
    for (final String path : paths) {
      files.add(new File(path));
    }
    return files;
  }

  private Set<String> stagedFileNames(final String uuid) throws IOException {
    final File taskDir = findDirectory(tempDir, uuid);
    assertNotNull("no staged directory for " + uuid, taskDir);
    final Set<String> names = new LinkedHashSet<>();
    final File[] files = taskDir.listFiles();
    if (files != null) {
      for (final File file : files) {
        if (file.isFile() && file.getName().endsWith(".tsfile")) {
          names.add(file.getName());
        }
      }
    }
    return names;
  }

  private ScenarioStats verifyScenario(final Scenario scenario, final List<File> targetFiles)
      throws Exception {
    final List<String> failures = new ArrayList<>();
    final ScenarioStats stats = verifyScenario(scenario, targetFiles, failures);
    if (!failures.isEmpty()) {
      fail(
          "scenario "
              + scenario.name
              + " has "
              + failures.size()
              + " mismatching series:\n  "
              + String.join("\n  ", failures)
              + "\nstaged content:\n  "
              + String.join("\n  ", describeStagedFiles(targetFiles)));
    }
    return stats;
  }

  private ScenarioStats verifyScenario(
      final Scenario scenario, final List<File> targetFiles, final List<String> failures)
      throws Exception {
    final List<StagedReader> readers = new ArrayList<>();
    try {
      for (final File file : targetFiles) {
        assertTrue("staged TsFile is missing: " + file, file.exists() && file.length() > 0);
        readers.add(new StagedReader(file, new TsFileReader(file)));
      }
      final ScenarioStats stats = new ScenarioStats(scenario.name);
      stats.fileCount = readers.size();
      for (final DeviceSpec device : scenario.devices) {
        for (final SeriesSpec series : device.series) {
          final long points = verifySeries(device, series, readers, failures);
          if (points >= 0) {
            stats.pointCount += points;
          }
          stats.seriesCount++;
        }
      }
      return stats;
    } finally {
      for (final StagedReader reader : readers) {
        reader.reader.close();
      }
    }
  }

  /** Lists the devices, measurements and chunk layout of each staged TsFile, for diagnostics. */
  private static List<String> describeStagedFiles(final List<File> files) throws IOException {
    final List<String> descriptions = new ArrayList<>();
    for (final File file : files) {
      try (final TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
        final StringBuilder builder = new StringBuilder(file.getName());
        builder.append("(len=").append(file.length()).append("): ");
        for (final Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry :
            reader.getAllTimeseriesMetadata(true).entrySet()) {
          builder.append(entry.getKey()).append('{');
          for (final TimeseriesMetadata metadata : entry.getValue()) {
            builder.append(metadata.getMeasurementId()).append('[');
            for (final IChunkMetadata chunkMetadata : metadata.getChunkMetadataList()) {
              builder
                  .append('@')
                  .append(chunkMetadata.getOffsetOfChunkHeader())
                  .append(':')
                  .append(chunkMetadata.getStatistics().getStartTime())
                  .append('-')
                  .append(chunkMetadata.getStatistics().getEndTime())
                  .append(',');
            }
            builder.append("] ");
          }
          builder.append("} ");
        }
        descriptions.add(builder.toString());
      }
    }
    return descriptions;
  }

  private static long verifySeries(
      final DeviceSpec device,
      final SeriesSpec series,
      final List<StagedReader> readers,
      final List<String> failures)
      throws Exception {
    final List<Object[]> expected = expectedPoints(series);
    final List<Object[]> actual = new ArrayList<>();
    final List<String> perFileCounts = new ArrayList<>();
    for (final StagedReader reader : readers) {
      // A fresh QueryExpression (and Path) per file: reusing one instance across readers can make
      // the later queries return nothing, so every read builds its own expression.
      final QueryExpression queryExpression =
          QueryExpression.create(
              Collections.singletonList(
                  new Path(device.device, series.measurement, device.aligned)),
              null);
      final int before = actual.size();
      final QueryDataSet dataSet = reader.reader.query(queryExpression);
      while (dataSet.hasNext()) {
        final RowRecord row = dataSet.next();
        final Field field = row.getField(0);
        assertNotNull(
            "null field read back for " + describe(device, series) + " at " + row.getTimestamp(),
            field);
        actual.add(new Object[] {row.getTimestamp(), readValue(field, series.dataType)});
      }
      perFileCounts.add(reader.file.getName() + "=" + (actual.size() - before));
    }

    // A series may span several staged files, one per time partition, and the pieces may have been
    // written in any order, so compare on the sorted union.
    actual.sort((left, right) -> Long.compare((Long) left[0], (Long) right[0]));
    final String where = describe(device, series) + " in " + perFileCounts;
    for (int i = 1; i < actual.size(); i++) {
      if ((Long) actual.get(i)[0] <= (Long) actual.get(i - 1)[0]) {
        failures.add("duplicate timestamp " + actual.get(i)[0] + " for " + where);
        return -1;
      }
    }
    if (expected.size() != actual.size()) {
      failures.add(
          "point count mismatch for "
              + where
              + ", expected "
              + expected.size()
              + " but was "
              + actual.size());
      return -1;
    }
    for (int i = 0; i < expected.size(); i++) {
      if (!expected.get(i)[0].equals(actual.get(i)[0])) {
        failures.add(
            "timestamp mismatch at index "
                + i
                + " for "
                + where
                + ", expected "
                + expected.get(i)[0]
                + " but was "
                + actual.get(i)[0]);
        return -1;
      }
      if (!expected.get(i)[1].equals(actual.get(i)[1])) {
        failures.add(
            "value mismatch at index "
                + i
                + " for "
                + where
                + ", expected "
                + expected.get(i)[1]
                + " but was "
                + actual.get(i)[1]);
        return -1;
      }
    }
    return expected.size();
  }

  private static String describe(final DeviceSpec device, final SeriesSpec series) {
    return device.device + "." + series.measurement + (device.aligned ? " (aligned)" : " (plain)");
  }

  private static List<Object[]> expectedPoints(final SeriesSpec series) {
    final List<Object[]> expected = new ArrayList<>();
    for (int chunkIndex = 0; chunkIndex < series.chunks.size(); chunkIndex++) {
      for (final long[] pageRange : series.chunks.get(chunkIndex)) {
        for (long time = pageRange[0]; time <= pageRange[1]; time++) {
          if (series.nullMode.isNull(time, chunkIndex)) {
            continue;
          }
          expected.add(new Object[] {time, valueFor(series.dataType, time)});
        }
      }
    }
    expected.sort((left, right) -> Long.compare((Long) left[0], (Long) right[0]));
    return expected;
  }

  // ------------------------------------------------------------------
  // Source TsFile construction
  // ------------------------------------------------------------------

  private static void writeSourceTsFile(final File tsFile, final List<DeviceSpec> devices)
      throws IOException {
    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      for (final DeviceSpec device : devices) {
        validate(device);
        writer.startChunkGroup(device.device);
        if (device.aligned) {
          writeAlignedDevice(writer, device);
        } else {
          writeNonAlignedDevice(writer, device);
        }
        writer.endChunkGroup();
      }
      writer.endFile();
    }
  }

  private static void validate(final DeviceSpec device) {
    if (!device.aligned) {
      return;
    }
    // Every measurement of an aligned device shares one time column, so the whole device is written
    // chunk by chunk with all value columns of that chunk, and every measurement must contribute
    // the
    // same chunk and page shape.
    final SeriesSpec reference = device.series.get(0);
    for (final SeriesSpec series : device.series) {
      assertEquals(
          "aligned measurements of " + device.device + " must share the chunk count",
          reference.chunks.size(),
          series.chunks.size());
      for (int i = 0; i < reference.chunks.size(); i++) {
        assertEquals(
            "aligned measurements of " + device.device + " must share the page shape",
            reference.chunks.get(i).length,
            series.chunks.get(i).length);
        for (int j = 0; j < reference.chunks.get(i).length; j++) {
          assertArrayEquals(
              "aligned measurements of " + device.device + " must share the page range",
              reference.chunks.get(i)[j],
              series.chunks.get(i)[j]);
        }
      }
    }
  }

  private static void writeNonAlignedDevice(final TsFileIOWriter writer, final DeviceSpec device)
      throws IOException {
    if (device.chunkOrder == ChunkOrder.SERIES_MAJOR) {
      for (final SeriesSpec series : device.series) {
        for (int chunkIndex = 0; chunkIndex < series.chunks.size(); chunkIndex++) {
          writeNonAlignedChunk(writer, series, chunkIndex);
        }
      }
    } else {
      final int chunkCount = maxChunkCount(device);
      for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
        for (final SeriesSpec series : device.series) {
          if (chunkIndex < series.chunks.size()) {
            writeNonAlignedChunk(writer, series, chunkIndex);
          }
        }
      }
    }
  }

  private static void writeNonAlignedChunk(
      final TsFileIOWriter writer, final SeriesSpec series, final int chunkIndex)
      throws IOException {
    final ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema(series));
    boolean wroteAnyPoint = false;
    for (final long[] pageRange : series.chunks.get(chunkIndex)) {
      boolean wroteThisPage = false;
      for (long time = pageRange[0]; time <= pageRange[1]; time++) {
        if (series.nullMode.isNull(time, chunkIndex)) {
          continue;
        }
        writeNonAlignedValue(chunkWriter, series.dataType, time, valueFor(series.dataType, time));
        wroteThisPage = true;
      }
      if (wroteThisPage) {
        chunkWriter.sealCurrentPage();
        wroteAnyPoint = true;
      }
    }
    if (wroteAnyPoint) {
      chunkWriter.writeToFileWriter(writer);
    }
  }

  private static void writeAlignedDevice(final TsFileIOWriter writer, final DeviceSpec device)
      throws IOException {
    // An aligned device owns a single time column, so one chunk of the device is written with every
    // value column of that chunk in the same writer.
    final int chunkCount = maxChunkCount(device);
    for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
      final AlignedChunkWriterImpl alignedWriter =
          new AlignedChunkWriterImpl(schemas(device.series));
      writeAlignedChunkPages(alignedWriter, device.series, chunkIndex);
      alignedWriter.writeToFileWriter(writer);
    }
  }

  private static void writeAlignedChunkPages(
      final AlignedChunkWriterImpl alignedWriter,
      final List<SeriesSpec> seriesList,
      final int chunkIndex)
      throws IOException {
    for (final long[] pageRange : seriesList.get(0).chunks.get(chunkIndex)) {
      for (long time = pageRange[0]; time <= pageRange[1]; time++) {
        for (final SeriesSpec series : seriesList) {
          writeAlignedValue(
              alignedWriter,
              series.dataType,
              time,
              valueFor(series.dataType, time),
              series.nullMode.isNull(time, chunkIndex));
        }
        alignedWriter.write(time);
      }
      alignedWriter.sealCurrentPage();
    }
  }

  private static int maxChunkCount(final DeviceSpec device) {
    int max = 0;
    for (final SeriesSpec series : device.series) {
      max = Math.max(max, series.chunks.size());
    }
    return max;
  }

  private static List<IMeasurementSchema> schemas(final List<SeriesSpec> series) {
    final List<IMeasurementSchema> schemas = new ArrayList<>();
    for (final SeriesSpec one : series) {
      schemas.add(schema(one));
    }
    return schemas;
  }

  private static MeasurementSchema schema(final SeriesSpec series) {
    return new MeasurementSchema(
        series.measurement, series.dataType, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
  }

  private static void writeNonAlignedValue(
      final ChunkWriterImpl writer, final TSDataType type, final long time, final Object value) {
    switch (type) {
      case INT32:
        writer.write(time, (Integer) value);
        break;
      case INT64:
        writer.write(time, (Long) value);
        break;
      case FLOAT:
        writer.write(time, (Float) value);
        break;
      case DOUBLE:
        writer.write(time, (Double) value);
        break;
      case BOOLEAN:
        writer.write(time, (Boolean) value);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        writer.write(time, (Binary) value);
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  private static void writeAlignedValue(
      final AlignedChunkWriterImpl writer,
      final TSDataType type,
      final long time,
      final Object value,
      final boolean isNull) {
    switch (type) {
      case INT32:
        writer.write(time, (Integer) value, isNull);
        break;
      case INT64:
        writer.write(time, (Long) value, isNull);
        break;
      case FLOAT:
        writer.write(time, (Float) value, isNull);
        break;
      case DOUBLE:
        writer.write(time, (Double) value, isNull);
        break;
      case BOOLEAN:
        writer.write(time, (Boolean) value, isNull);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        writer.write(time, (Binary) value, isNull);
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  private static Object valueFor(final TSDataType type, final long time) {
    switch (type) {
      case INT32:
        return (int) time;
      case INT64:
        return time;
      case FLOAT:
        return (float) time;
      case DOUBLE:
        return (double) time;
      case BOOLEAN:
        return time % 2 == 0;
      case TEXT:
      case STRING:
        return new Binary("v" + time, StandardCharsets.UTF_8);
      case BLOB:
        return new Binary(("blob" + time).getBytes(StandardCharsets.UTF_8));
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  private static Object readValue(final Field field, final TSDataType type) {
    switch (type) {
      case INT32:
        return field.getIntV();
      case INT64:
        return field.getLongV();
      case FLOAT:
        return field.getFloatV();
      case DOUBLE:
        return field.getDoubleV();
      case BOOLEAN:
        return field.getBoolV();
      case TEXT:
      case STRING:
      case BLOB:
        return field.getBinaryV();
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  // ------------------------------------------------------------------
  // Shape helpers
  // ------------------------------------------------------------------

  private static long[] page(final long startTime, final long endTime) {
    return new long[] {startTime, endTime};
  }

  private static List<long[][]> chunkList(final long[][]... chunks) {
    return Arrays.asList(chunks);
  }

  /** Consecutive pages of {@code pointsPerPage} points covering {@code totalPoints} points. */
  private static long[][] pageChain(
      final long startTime, final int totalPoints, final int pointsPerPage) {
    final int pageCount = (totalPoints + pointsPerPage - 1) / pointsPerPage;
    final long[][] pages = new long[pageCount][];
    long time = startTime;
    for (int i = 0; i < pageCount; i++) {
      final int points = Math.min(pointsPerPage, totalPoints - i * pointsPerPage);
      pages[i] = page(time, time + points - 1);
      time += points;
    }
    return pages;
  }

  /**
   * Three chunks: a whole chunk inside one coarse partition, a chunk whose range crosses a
   * partition boundary while each of its pages stays inside a partition, and a chunk holding a
   * single page that crosses a boundary (the decode + re-encode path with {@code sealCurrentPage}).
   */
  private static List<long[][]> richShape(final long base) {
    return chunkList(
        new long[][] {
          page(base, base + 49), page(base + 50, base + 99), page(base + 100, base + 149)
        },
        new long[][] {page(base + 950, base + 999), page(base + 1000, base + 1049)},
        new long[][] {page(base + 1900, base + 2100)});
  }

  /** A chunk with many pages of mixed sizes that straddles one partition boundary. */
  private static List<long[][]> manyPageShape(final long base) {
    return chunkList(
        new long[][] {
          page(base + 900, base + 949),
          page(base + 950, base + 999),
          page(base + 1000, base + 1049),
          page(base + 1050, base + 1099),
          page(base + 1100, base + 1199),
          page(base + 1200, base + 1209),
          page(base + 1210, base + 1219),
          page(base + 1220, base + 1299),
          page(base + 1300, base + 1304),
          page(base + 1305, base + 1309)
        });
  }

  /** 25_000 points per series: three chunks inside a coarse partition and one crossing chunk. */
  private static List<long[][]> bulkShape() {
    return chunkList(
        pageChain(0L, 5_000, 1_000),
        pageChain(70_000L, 5_000, 1_000),
        pageChain(140_000L, 5_000, 1_000),
        pageChain(195_000L, 10_000, 1_000));
  }

  /** Fine shapes used with {@link #FINE_PARTITION_INTERVAL}. */
  private static List<long[][]> fineSpreadShape(final long base) {
    return chunkList(
        new long[][] {
          page(base, base + 9),
          page(base + 100, base + 109),
          page(base + 200, base + 209),
          page(base + 300, base + 309)
        });
  }

  private static List<long[][]> fineChunkCrossingShape(final long base) {
    return chunkList(
        new long[][] {
          page(base + 90, base + 99), page(base + 100, base + 109), page(base + 200, base + 209)
        });
  }

  private static List<long[][]> finePageCrossingShape(final long base) {
    return chunkList(new long[][] {page(base + 90, base + 290)});
  }

  private static List<long[][]> fineMixedShape(final long base) {
    return chunkList(new long[][] {page(base + 90, base + 110), page(base + 111, base + 120)});
  }

  private static List<DeviceSpec> boundaryPair(
      final String name, final List<long[][]> alignedShape, final List<long[][]> plainShape) {
    return Arrays.asList(
        alignedDevice(
            "aligned_" + name,
            ChunkOrder.CHUNK_MAJOR,
            series("m0", TSDataType.INT64, NullMode.NONE, alignedShape),
            series("m1", TSDataType.DOUBLE, NullMode.ALTERNATE, alignedShape)),
        nonAlignedDevice(
            "plain_" + name,
            ChunkOrder.CHUNK_MAJOR,
            series("m0", TSDataType.INT32, NullMode.NONE, plainShape),
            series("m1", TSDataType.TEXT, NullMode.SPARSE, plainShape)));
  }

  // ------------------------------------------------------------------
  // Model
  // ------------------------------------------------------------------

  /** Chunk interleaving of a non-aligned device; aligned devices always share one time column. */
  private enum ChunkOrder {
    /** All chunks of one series, then all chunks of the next one. */
    SERIES_MAJOR,
    /** The k-th chunk of every series, then the (k+1)-th chunk of every series. */
    CHUNK_MAJOR
  }

  /** How the split result is cut into pieces and in which order the pieces are handed over. */
  private static final class DispatchPlan {
    /** Pieces per time partition; {@code 0} means one piece per chunk. */
    final int piecesPerPartition;

    final boolean reversePartitions;
    final boolean reversePieces;

    DispatchPlan(
        final int piecesPerPartition,
        final boolean reversePartitions,
        final boolean reversePieces) {
      this.piecesPerPartition = piecesPerPartition;
      this.reversePartitions = reversePartitions;
      this.reversePieces = reversePieces;
    }

    @Override
    public String toString() {
      return "piecesPerPartition="
          + piecesPerPartition
          + ",reversePartitions="
          + reversePartitions
          + ",reversePieces="
          + reversePieces;
    }
  }

  private enum NullMode {
    NONE,
    ALTERNATE,
    SPARSE,
    ALL,
    ALTERNATE_CHUNK;

    boolean isNull(final long time, final int chunkIndex) {
      switch (this) {
        case ALTERNATE:
          return time % 2 != 0;
        case SPARSE:
          return time % 7 == 3;
        case ALL:
          return true;
        case ALTERNATE_CHUNK:
          return chunkIndex % 2 == 1;
        case NONE:
        default:
          return false;
      }
    }
  }

  private static final class SeriesSpec {
    final String measurement;
    final TSDataType dataType;
    final NullMode nullMode;

    /** Outer list = chunks; inner list = pages; each page = [startTime, endTime] inclusive. */
    final List<long[][]> chunks;

    SeriesSpec(
        final String measurement,
        final TSDataType dataType,
        final NullMode nullMode,
        final List<long[][]> chunks) {
      this.measurement = measurement;
      this.dataType = dataType;
      this.nullMode = nullMode;
      this.chunks = chunks;
    }
  }

  private static final class DeviceSpec {
    final IDeviceID device;
    final boolean aligned;
    final ChunkOrder chunkOrder;
    final List<SeriesSpec> series;

    DeviceSpec(
        final IDeviceID device,
        final boolean aligned,
        final ChunkOrder chunkOrder,
        final List<SeriesSpec> series) {
      this.device = device;
      this.aligned = aligned;
      this.chunkOrder = chunkOrder;
      this.series = series;
    }
  }

  private static final class Scenario {
    final String name;
    final long partitionInterval;
    final List<DeviceSpec> devices;
    final DispatchPlan dispatchPlan;
    final long minPoints;

    Scenario(
        final String name,
        final long partitionInterval,
        final List<DeviceSpec> devices,
        final DispatchPlan dispatchPlan,
        final long minPoints) {
      this.name = name;
      this.partitionInterval = partitionInterval;
      this.devices = devices;
      this.dispatchPlan = dispatchPlan;
      this.minPoints = minPoints;
    }
  }

  private static final class StagedReader {
    final File file;
    final TsFileReader reader;

    StagedReader(final File file, final TsFileReader reader) {
      this.file = file;
      this.reader = reader;
    }
  }

  private static final class ScenarioStats {
    final String name;
    int seriesCount;
    long pointCount;
    int fileCount;
    int pieceCount;

    ScenarioStats(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return "[split-coverage] scenario="
          + name
          + " series="
          + seriesCount
          + " points="
          + pointCount
          + " pieces="
          + pieceCount
          + " stagedFiles="
          + fileCount;
    }
  }

  // ------------------------------------------------------------------
  // Builders and file helpers
  // ------------------------------------------------------------------

  private static SeriesSpec series(
      final String measurement,
      final TSDataType dataType,
      final NullMode nullMode,
      final List<long[][]> chunks) {
    return new SeriesSpec(measurement, dataType, nullMode, chunks);
  }

  private static DeviceSpec alignedDevice(
      final String name, final ChunkOrder chunkOrder, final SeriesSpec... series) {
    return new DeviceSpec(deviceId(name), true, chunkOrder, Arrays.asList(series));
  }

  private static DeviceSpec nonAlignedDevice(
      final String name, final ChunkOrder chunkOrder, final SeriesSpec... series) {
    return new DeviceSpec(deviceId(name), false, chunkOrder, Arrays.asList(series));
  }

  private static IDeviceID deviceId(final String name) {
    return new StringArrayDeviceID("root", "split_coverage", name);
  }

  private static void setPageConfig(final int pageSize, final int maxPoints) {
    final TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    tsFileConfig.setPageSizeInByte(pageSize);
    tsFileConfig.setMaxNumberOfPointsInPage(maxPoints);
  }

  private static void setTimePartitionInterval(final long interval) {
    CommonDescriptor.getInstance().getConfig().setTimePartitionInterval(interval);
    TimePartitionUtils.setTimePartitionInterval(interval);
  }

  private static File findDirectory(final File root, final String name) {
    if (!root.isDirectory()) {
      return null;
    }
    final File[] files = root.listFiles();
    if (files == null) {
      return null;
    }
    for (final File file : files) {
      if (file.isDirectory()) {
        if (file.getName().equals(name)) {
          return file;
        }
        final File found = findDirectory(file, name);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
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
}
