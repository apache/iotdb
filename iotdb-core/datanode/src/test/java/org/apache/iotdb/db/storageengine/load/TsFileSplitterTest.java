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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;

import com.sun.management.ThreadMXBean;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.encoder.PlainEncoder;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MeasurementMetadataIndexEntry;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.apache.tsfile.write.writer.tsmiterator.TSMIterator;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class TsFileSplitterTest {

  @Test
  public void testRejectedDeletionStopsSplitting() throws Exception {
    final File source = Files.createTempFile("rejected-load-deletion", ".tsfile").toFile();
    final File mods = ModificationFile.getExclusiveMods(source);
    try {
      try (final TsFileIOWriter writer = new TsFileIOWriter(source)) {
        writer.startChunkGroup(new StringArrayDeviceID("root", "sg", "d1"));
        final ChunkWriterImpl chunkWriter =
            new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT32));
        chunkWriter.write(1L, 1);
        chunkWriter.writeToFileWriter(writer);
        writer.endChunkGroup();
        writer.endFile();
      }
      try (final ModificationFile modificationFile = new ModificationFile(mods, false)) {
        modificationFile.write(new TreeDeletionEntry(new MeasurementPath("root.sg.d1.s1"), 0L, 1L));
        modificationFile.write(new TreeDeletionEntry(new MeasurementPath("root.sg.d1.s1"), 2L, 3L));
      }
      final AtomicInteger consumedDeletions = new AtomicInteger();
      final TsFileSplitter splitter =
          new TsFileSplitter(
              source,
              data -> {
                if (data instanceof DeletionData) {
                  consumedDeletions.incrementAndGet();
                  return false;
                }
                return true;
              });
      Assert.assertThrows(IllegalStateException.class, splitter::splitTsFileByDataPartition);
      Assert.assertEquals(1, consumedDeletions.get());
    } finally {
      Files.deleteIfExists(mods.toPath());
      Files.deleteIfExists(source.toPath());
    }
  }

  // Verify the splitter initializes the v3 deserialize configuration for a valid v3 TsFile.
  @Test
  public void testSplitV3TsFile() throws Exception {
    final File sourceTsFile = constructV3TsFile();
    final List<ChunkData> chunkDataList = new ArrayList<>();

    try {
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(sourceTsFile.getAbsolutePath())) {
        Assert.assertEquals(1, reader.getAllTimeseriesMetadata(true).size());
      }

      // Verify the buffered reader initializes the v3 deserialize configuration before reading
      // metadata.
      new TsFileSplitter(
              sourceTsFile,
              tsFileData -> {
                if (tsFileData instanceof ChunkData) {
                  chunkDataList.add((ChunkData) tsFileData);
                }
                return true;
              })
          .splitTsFileByDataPartition();
      Assert.assertEquals(1, chunkDataList.size());
    } finally {
      Assert.assertTrue(sourceTsFile.delete());
    }
  }

  @Test
  public void testSplitRejectsMoreTimePartitionsThanTheConfiguredLimit() throws Exception {
    final File sourceTsFile = new File("split-partition-limit-source.tsfile");
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final int originalPartitionSlotsLimit = config.getLoadTsFileSpiltPartitionMaxSize();
    try {
      config.setLoadTsFileSpiltPartitionMaxSize(2);
      writeTableTsFileWithOnePointPerTimePartition(sourceTsFile, 3);

      try {
        new TsFileSplitter(sourceTsFile, tsFileData -> true).splitTsFileByDataPartition();
        Assert.fail("expected the splitter to reject more time partitions than the limit");
      } catch (final LoadFileException e) {
        // expected: the third partition crosses the configured limit
      }
    } finally {
      config.setLoadTsFileSpiltPartitionMaxSize(originalPartitionSlotsLimit);
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
    }
  }

  @Test
  public void testSplitAlignedChunkAcrossManyTimePartitionsDecodesEachPageOnce() throws Exception {
    // One point per time partition in a single chunk group makes every value page span as many
    // time partitions as the page holds points. Such a page must be decoded once and shared by the
    // partitions that it covers: decoding it once per partition allocates one page-sized array per
    // partition, i.e. O(points * partitions) objects, which exhausts the DataNode heap when a
    // TsFile stores its points one time partition apart.
    final int pointsPerPage = 1024;
    final int pointCount = 2 * pointsPerPage;
    final File sourceTsFile = new File("split-aligned-many-partitions-source.tsfile");
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final int originalPartitionSlotsLimit = config.getLoadTsFileSpiltPartitionMaxSize();
    final int originalMaxPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    try {
      config.setLoadTsFileSpiltPartitionMaxSize(pointCount + 1);
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pointsPerPage);
      writeTableTsFileWithOnePointPerTimePartition(sourceTsFile, pointCount);

      final AtomicInteger emittedChunkDataCount = new AtomicInteger();
      final long allocatedBytes =
          splitAndMeasureAllocatedBytes(sourceTsFile, emittedChunkDataCount);

      Assert.assertEquals(pointCount, emittedChunkDataCount.get());
      // Decoding the pages per partition too and recomputing the partition bounds per point both
      // make this O(points * partitions) in allocations, which is hundreds of megabytes here.
      Assert.assertTrue(
          "splitting "
              + pointCount
              + " single-point time partitions allocated "
              + allocatedBytes
              + " bytes",
          allocatedBytes < 128L * 1024 * 1024);
    } finally {
      config.setLoadTsFileSpiltPartitionMaxSize(originalPartitionSlotsLimit);
      TSFileDescriptor.getInstance()
          .getConfig()
          .setMaxNumberOfPointsInPage(originalMaxPointsInPage);
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
    }
  }

  private long splitAndMeasureAllocatedBytes(
      final File sourceTsFile, final AtomicInteger emittedChunkDataCount) throws Exception {
    final java.lang.management.ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    Assume.assumeTrue(
        "the JVM does not report the allocations of a thread",
        threadMXBean instanceof ThreadMXBean);
    final long allocatedBytesBefore =
        ((ThreadMXBean) threadMXBean).getCurrentThreadAllocatedBytes();
    new TsFileSplitter(
            sourceTsFile,
            tsFileData -> {
              if (tsFileData instanceof ChunkData) {
                emittedChunkDataCount.incrementAndGet();
              }
              return true;
            })
        .splitTsFileByDataPartition();
    return ((ThreadMXBean) threadMXBean).getCurrentThreadAllocatedBytes() - allocatedBytesBefore;
  }

  private void writeTableTsFileWithOnePointPerTimePartition(final File tsFile, final int pointCount)
      throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }
    final long baseTime =
        TimePartitionUtils.getTimePartitionSlot(1_700_000_000_000L).getStartTime();
    final long timePartitionInterval = TimePartitionUtils.getTimePartitionInterval();

    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(createSchema());
      writer.startChunkGroup(new StringArrayDeviceID("table1", "tagA"));

      final AlignedChunkWriterImpl chunkWriter =
          new AlignedChunkWriterImpl(
              Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT64)));
      for (int i = 0; i < pointCount; i++) {
        final long time = baseTime + i * timePartitionInterval;
        chunkWriter.getTimeChunkWriter().write(time);
        chunkWriter.getValueChunkWriterByIndex(0).write(time, (long) i, false);
      }
      chunkWriter.writeToFileWriter(writer);

      writer.endChunkGroup();
      writer.endFile();
    }
  }

  private File constructV3TsFile() throws IOException {
    final File tsFile = Files.createTempFile("v3-tsfile-splitter", ".tsfile").toFile();
    final IDeviceID deviceID = new PlainDeviceID("root.sg.d1");
    final TimeseriesMetadata timeseriesMetadata;
    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.startChunkGroup(deviceID);
      final ChunkWriterImpl chunkWriter =
          new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT32));
      chunkWriter.write(1, 1);
      chunkWriter.writeToFileWriter(writer);
      writer.endChunkGroup();

      final List<IChunkMetadata> chunkMetadataList =
          new ArrayList<IChunkMetadata>(writer.getDeviceChunkMetadataMap().get(deviceID));
      timeseriesMetadata = TSMIterator.constructOneTimeseriesMetadata("s1", chunkMetadataList);
    }

    final byte[] v3TsFileData = Files.readAllBytes(tsFile.toPath());
    v3TsFileData[TSFileConfig.MAGIC_STRING.getBytes().length] = TSFileConfig.VERSION_NUMBER_V3;
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    outputStream.write(v3TsFileData);

    final long metaOffset = outputStream.size();
    outputStream.write(MetaMarker.SEPARATOR);
    final long timeseriesMetadataOffset = outputStream.size();
    timeseriesMetadata.serializeTo(outputStream);

    final long measurementIndexOffset = outputStream.size();
    final MetadataIndexNode measurementIndexNode =
        new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
    measurementIndexNode.addEntry(
        new MeasurementMetadataIndexEntry("s1", timeseriesMetadataOffset));
    measurementIndexNode.setEndOffset(measurementIndexOffset);
    measurementIndexNode.serializeTo(outputStream);

    final MetadataIndexNode deviceIndexNode =
        new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
    deviceIndexNode.addEntry(new DeviceMetadataIndexEntry(deviceID, measurementIndexOffset));
    deviceIndexNode.setEndOffset(outputStream.size());

    int metadataSize = deviceIndexNode.serializeTo(outputStream);
    metadataSize += ReadWriteIOUtils.write(metaOffset, outputStream);
    ReadWriteIOUtils.write(metadataSize, outputStream);
    outputStream.write(TSFileConfig.MAGIC_STRING.getBytes());
    Files.write(tsFile.toPath(), outputStream.toByteArray());
    return tsFile;
  }

  @Test
  public void testSplitAlignedValueChunkWithUndecodedPagesPreservesValues() throws Exception {
    assertAlignedValueChunkSplitPreservesValues(2);
  }

  @Test
  public void testSplitAlignedValueChunkSpanningTimePartitionsPreservesValues() throws Exception {
    // A page that covers two time partitions has to be decoded and written once per partition. The
    // partition ranges are located by binary search, so this also checks that no point is dropped
    // or duplicated at the partition boundary.
    assertAlignedValueChunkSplitPreservesValues(4);
  }

  private void assertAlignedValueChunkSplitPreservesValues(final int maxNumberOfPointsInPage)
      throws Exception {
    final File sourceTsFile =
        new File("split-table-aligned-values-" + maxNumberOfPointsInPage + "-source.tsfile");
    final File targetTsFile =
        new File("split-table-aligned-values-" + maxNumberOfPointsInPage + "-target.tsfile");
    final IDeviceID deviceID = new StringArrayDeviceID("table1", "tagA");
    final int pointsPerPartition = 2;
    final int partitionCount = 2;
    final long baseTime =
        TimePartitionUtils.getTimePartitionSlot(1_700_000_000_000L).getStartTime();
    final long timePartitionInterval = TimePartitionUtils.getTimePartitionInterval();
    final int pointCount = pointsPerPartition * partitionCount;
    final long[] times = new long[pointCount];
    final long[] values = new long[pointCount];

    final int previousMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    try {
      TSFileDescriptor.getInstance()
          .getConfig()
          .setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      try (final TsFileIOWriter writer = new TsFileIOWriter(sourceTsFile)) {
        writer.setSchema(createSchema());
        writer.startChunkGroup(deviceID);
        final AlignedChunkWriterImpl chunkWriter =
            new AlignedChunkWriterImpl(
                Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT64)));
        for (int partition = 0; partition < partitionCount; partition++) {
          for (int i = 0; i < pointsPerPartition; i++) {
            final int index = partition * pointsPerPartition + i;
            times[index] = baseTime + partition * timePartitionInterval + i;
            values[index] = index;
            chunkWriter.getTimeChunkWriter().write(times[index]);
            chunkWriter.getValueChunkWriterByIndex(0).write(times[index], values[index], false);
          }
        }
        chunkWriter.writeToFileWriter(writer);
        writer.endChunkGroup();
        writer.endFile();
      }

      final List<ChunkData> emittedChunkDataList = new ArrayList<>();
      new TsFileSplitter(
              sourceTsFile,
              tsFileData -> {
                if (tsFileData instanceof ChunkData) {
                  emittedChunkDataList.add((ChunkData) tsFileData);
                }
                return true;
              })
          .splitTsFileByDataPartition();

      Assert.assertEquals(partitionCount, emittedChunkDataList.size());
      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
      try (final TsFileIOWriter writer = new TsFileIOWriter(targetTsFile)) {
        writer.setSchema(createSchema());
        writer.startChunkGroup(deviceID);
        for (final ChunkData chunkData : emittedChunkDataList) {
          writeSerializedChunkDataToWriter(chunkData, writer);
        }
        writer.endChunkGroup();
        writer.endFile();
      }

      try (final TsFileReader reader = new TsFileReader(targetTsFile)) {
        final QueryDataSet dataSet =
            reader.query(
                QueryExpression.create(
                    Collections.singletonList(new Path(deviceID, "s1", true)), null));
        int index = 0;
        while (dataSet.hasNext()) {
          final RowRecord row = dataSet.next();
          Assert.assertEquals(times[index], row.getTimestamp());
          final Field field = row.getField(0);
          Assert.assertNotNull(field);
          Assert.assertEquals(values[index], field.getLongV());
          index++;
        }
        Assert.assertEquals(pointCount, index);
      }
    } finally {
      TSFileDescriptor.getInstance()
          .getConfig()
          .setMaxNumberOfPointsInPage(previousMaxNumberOfPointsInPage);
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
    }
  }

  @Test
  public void testSplitTableTimeOnlyAlignedChunk() throws Exception {
    final File sourceTsFile = new File("split-table-time-only-source.tsfile");
    final File targetTsFile = new File("split-table-time-only-target.tsfile");
    final IDeviceID deviceID = new StringArrayDeviceID("table1", "tagA");

    try {
      writeTableTsFileWithTimeOnlyChunk(sourceTsFile, deviceID, 100, 101);

      final List<ChunkData> emittedChunkDataList = new ArrayList<>();
      final TsFileSplitter splitter =
          new TsFileSplitter(
              sourceTsFile,
              tsFileData -> {
                if (tsFileData instanceof ChunkData) {
                  emittedChunkDataList.add((ChunkData) tsFileData);
                }
                return true;
              });
      splitter.splitTsFileByDataPartition();

      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
      try (final TsFileIOWriter writer = new TsFileIOWriter(targetTsFile)) {
        writer.setSchema(createSchema());
        IDeviceID currentDeviceID = null;
        for (final ChunkData chunkData : emittedChunkDataList) {
          if (!Objects.equals(currentDeviceID, chunkData.getDevice())) {
            if (Objects.nonNull(currentDeviceID)) {
              writer.endChunkGroup();
            }
            writer.startChunkGroup(chunkData.getDevice());
            currentDeviceID = chunkData.getDevice();
          }

          writeSerializedChunkDataToWriter(chunkData, writer);
        }
        if (Objects.nonNull(currentDeviceID)) {
          writer.endChunkGroup();
        }
        writer.endFile();
      }

      Assert.assertEquals(1, emittedChunkDataList.size());
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(targetTsFile.getAbsolutePath())) {
        final List<AbstractAlignedChunkMetadata> chunkMetadataList =
            reader.getAlignedChunkMetadata(deviceID, false);
        Assert.assertEquals(1, chunkMetadataList.size());
        Assert.assertEquals(
            2, chunkMetadataList.get(0).getTimeChunkMetadata().getStatistics().getCount());
        Assert.assertTrue(chunkMetadataList.get(0).getValueChunkMetadataList().isEmpty());
      }
    } finally {
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
    }
  }

  @Test
  public void testSplitTableTimeOnlyAlignedChunkAtLongMaxPartitionEnd() throws Exception {
    final File sourceTsFile = new File("split-table-time-only-long-max-source.tsfile");
    final File targetTsFile = new File("split-table-time-only-long-max-target.tsfile");
    final IDeviceID deviceID = new StringArrayDeviceID("table1", "tagA");
    final long lastPartitionStartTime =
        TimePartitionUtils.getTimePartitionSlot(Long.MAX_VALUE).getStartTime();

    try {
      writeTableTsFileWithTimeOnlyChunk(
          sourceTsFile,
          deviceID,
          lastPartitionStartTime - 1,
          lastPartitionStartTime,
          Long.MAX_VALUE);

      final List<ChunkData> emittedChunkDataList = new ArrayList<>();
      final TsFileSplitter splitter =
          new TsFileSplitter(
              sourceTsFile,
              tsFileData -> {
                if (tsFileData instanceof ChunkData) {
                  emittedChunkDataList.add((ChunkData) tsFileData);
                }
                return true;
              });
      splitter.splitTsFileByDataPartition();

      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
      try (final TsFileIOWriter writer = new TsFileIOWriter(targetTsFile)) {
        writer.setSchema(createSchema());
        writer.startChunkGroup(deviceID);
        for (final ChunkData chunkData : emittedChunkDataList) {
          writeSerializedChunkDataToWriter(chunkData, writer);
        }
        writer.endChunkGroup();
        writer.endFile();
      }

      Assert.assertEquals(2, emittedChunkDataList.size());
      Assert.assertEquals(
          TimePartitionUtils.getTimePartitionSlot(lastPartitionStartTime - 1),
          emittedChunkDataList.get(0).getTimePartitionSlot());
      Assert.assertEquals(
          new TTimePartitionSlot(lastPartitionStartTime),
          emittedChunkDataList.get(1).getTimePartitionSlot());
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(targetTsFile.getAbsolutePath())) {
        final List<AbstractAlignedChunkMetadata> chunkMetadataList =
            reader.getAlignedChunkMetadata(deviceID, false);
        Assert.assertEquals(2, chunkMetadataList.size());
        Assert.assertEquals(
            3,
            chunkMetadataList.stream()
                .mapToLong(metadata -> metadata.getTimeChunkMetadata().getStatistics().getCount())
                .sum());
      }
    } finally {
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
    }
  }

  @Test
  public void testSplitTableTimeOnlyAlignedChunkAtExactLongMaxUpperBound() throws Exception {
    final File sourceTsFile = new File("split-table-time-only-exact-long-max-source.tsfile");
    final File targetTsFile = new File("split-table-time-only-exact-long-max-target.tsfile");
    final IDeviceID deviceID = new StringArrayDeviceID("table1", "tagA");
    final long previousTimePartitionOrigin =
        CommonDescriptor.getInstance().getConfig().getTimePartitionOrigin();
    final long previousTimePartitionInterval =
        CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();

    try {
      CommonDescriptor.getInstance().getConfig().setTimePartitionOrigin(0);
      CommonDescriptor.getInstance().getConfig().setTimePartitionInterval(1);
      TimePartitionUtils.setTimePartitionOrigin(0);
      TimePartitionUtils.setTimePartitionInterval(1);

      writeTableTsFileWithTimeOnlyChunk(sourceTsFile, deviceID, Long.MAX_VALUE - 1, Long.MAX_VALUE);

      final List<ChunkData> emittedChunkDataList = new ArrayList<>();
      final TsFileSplitter splitter =
          new TsFileSplitter(
              sourceTsFile,
              tsFileData -> {
                if (tsFileData instanceof ChunkData) {
                  emittedChunkDataList.add((ChunkData) tsFileData);
                }
                return true;
              });
      splitter.splitTsFileByDataPartition();

      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
      try (final TsFileIOWriter writer = new TsFileIOWriter(targetTsFile)) {
        writer.setSchema(createSchema());
        writer.startChunkGroup(deviceID);
        for (final ChunkData chunkData : emittedChunkDataList) {
          writeSerializedChunkDataToWriter(chunkData, writer);
        }
        writer.endChunkGroup();
        writer.endFile();
      }

      Assert.assertEquals(2, emittedChunkDataList.size());
      Assert.assertEquals(
          new TTimePartitionSlot(Long.MAX_VALUE - 1),
          emittedChunkDataList.get(0).getTimePartitionSlot());
      Assert.assertEquals(
          new TTimePartitionSlot(Long.MAX_VALUE),
          emittedChunkDataList.get(1).getTimePartitionSlot());
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(targetTsFile.getAbsolutePath())) {
        final List<AbstractAlignedChunkMetadata> chunkMetadataList =
            reader.getAlignedChunkMetadata(deviceID, false);
        Assert.assertEquals(2, chunkMetadataList.size());
        Assert.assertEquals(
            2,
            chunkMetadataList.stream()
                .mapToLong(metadata -> metadata.getTimeChunkMetadata().getStatistics().getCount())
                .sum());
      }
    } finally {
      TimePartitionUtils.setTimePartitionOrigin(previousTimePartitionOrigin);
      TimePartitionUtils.setTimePartitionInterval(previousTimePartitionInterval);
      CommonDescriptor.getInstance()
          .getConfig()
          .setTimePartitionOrigin(previousTimePartitionOrigin);
      CommonDescriptor.getInstance()
          .getConfig()
          .setTimePartitionInterval(previousTimePartitionInterval);
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
    }
  }

  private static final List<String> BATCHED_MEASUREMENTS =
      Arrays.asList("s1", "s2", "s3", "s4", "s5");

  private static final List<List<String>> BATCHED_MEASUREMENT_BATCHES =
      Arrays.asList(
          Arrays.asList("s1", "s2"), Arrays.asList("s3", "s4"), Collections.singletonList("s5"));

  /**
   * A batched aligned TsFile holds one time chunk per time window, but the value chunks of the
   * remaining series batches are appended behind all time chunks. That is what the compaction
   * writer emits when {@code compactionMaxAlignedSeriesNumInOneBatch} is smaller than the series
   * count, and it makes the splitter flush the chunk data of a time chunk several times.
   */
  @Test
  public void testSplitBatchedAlignedChunkAcrossTimePartitions() throws Exception {
    final File sourceTsFile = new File("split-batched-aligned-source.tsfile");
    final IDeviceID deviceID = new StringArrayDeviceID("table1", "tagA");
    final int previousMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    final long firstPartitionStartTime =
        TimePartitionUtils.getTimePartitionSlot(1_700_000_000_000L).getStartTime();
    final long timePartitionInterval = TimePartitionUtils.getTimePartitionInterval();
    final List<List<Long>> timeWindows =
        Arrays.asList(
            Arrays.asList(
                firstPartitionStartTime, firstPartitionStartTime + 1, firstPartitionStartTime + 2),
            Arrays.asList(
                firstPartitionStartTime + timePartitionInterval,
                firstPartitionStartTime + timePartitionInterval + 1));
    final File[] targetTsFiles = {
      new File("split-batched-aligned-target-0.tsfile"),
      new File("split-batched-aligned-target-1.tsfile")
    };
    try {
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(2);
      writeBatchedAlignedTsFile(
          sourceTsFile,
          Collections.singletonList(deviceID),
          timeWindows,
          BATCHED_MEASUREMENT_BATCHES,
          Collections.emptyMap(),
          Collections.emptyMap());

      final List<ChunkData> emittedChunkDataList = splitSourceTsFile(sourceTsFile);
      // The splitter flushes the chunk data at every series batch, so it is emitted several times.
      Assert.assertTrue(emittedChunkDataList.size() > 2);
      for (final ChunkData chunkData : emittedChunkDataList) {
        assertNoDuplicatedChunk(chunkData);
      }

      final Map<TTimePartitionSlot, List<ChunkData>> chunkDataByPartition =
          groupChunkDataByPartition(emittedChunkDataList);
      Assert.assertEquals(2, chunkDataByPartition.size());

      final List<ChunkData> firstPartitionChunkDataList =
          chunkDataByPartition.get(new TTimePartitionSlot(firstPartitionStartTime));
      Assert.assertNotNull(firstPartitionChunkDataList);
      Assert.assertTrue(firstPartitionChunkDataList.size() > 1);
      assertBatchedAlignedTarget(
          writeTargetTsFile(targetTsFiles[0], firstPartitionChunkDataList),
          deviceID,
          new long[] {
            firstPartitionStartTime, firstPartitionStartTime + 1, firstPartitionStartTime + 2
          },
          1,
          Collections.emptySet());

      final List<ChunkData> secondPartitionChunkDataList =
          chunkDataByPartition.get(
              new TTimePartitionSlot(firstPartitionStartTime + timePartitionInterval));
      Assert.assertNotNull(secondPartitionChunkDataList);
      Assert.assertTrue(secondPartitionChunkDataList.size() > 1);
      assertBatchedAlignedTarget(
          writeTargetTsFile(targetTsFiles[1], secondPartitionChunkDataList),
          deviceID,
          new long[] {
            firstPartitionStartTime + timePartitionInterval,
            firstPartitionStartTime + timePartitionInterval + 1
          },
          1,
          Collections.emptySet());
    } finally {
      TSFileDescriptor.getInstance()
          .getConfig()
          .setMaxNumberOfPointsInPage(previousMaxNumberOfPointsInPage);
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      for (final File targetTsFile : targetTsFiles) {
        if (targetTsFile.exists()) {
          Assert.assertTrue(targetTsFile.delete());
        }
      }
    }
  }

  /**
   * The same batched layout mixed with the remaining chunk shapes: the first time window stays
   * inside one time partition and carries empty value chunks, while the second window crosses the
   * partition boundary and carries the all-null (empty page) value chunks.
   */
  @Test
  public void testSplitBatchedAlignedChunkWithEmptyAndNullValueChunks() throws Exception {
    final File sourceTsFile = new File("split-batched-aligned-special-source.tsfile");
    final IDeviceID deviceID = new StringArrayDeviceID("table1", "tagA");
    final int previousMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    final long firstPartitionStartTime =
        TimePartitionUtils.getTimePartitionSlot(1_700_100_000_000L).getStartTime();
    final long timePartitionInterval = TimePartitionUtils.getTimePartitionInterval();
    final long crossedTime = firstPartitionStartTime + timePartitionInterval - 1;
    final long nextPartitionStartTime = firstPartitionStartTime + timePartitionInterval;
    final List<List<Long>> timeWindows =
        Arrays.asList(
            Collections.singletonList(firstPartitionStartTime + 3),
            Arrays.asList(crossedTime, nextPartitionStartTime));
    final Map<Integer, Set<String>> emptyMeasurementIdsByWindow =
        Collections.singletonMap(0, new HashSet<>(Arrays.asList("s4", "s5")));
    final Map<Integer, Set<String>> nullMeasurementIdsByWindow =
        Collections.singletonMap(1, new HashSet<>(Arrays.asList("s4", "s5")));
    final Set<String> nullMeasurements = new HashSet<>(Arrays.asList("s4", "s5"));
    final File[] targetTsFiles = {
      new File("split-batched-aligned-special-target-0.tsfile"),
      new File("split-batched-aligned-special-target-1.tsfile")
    };
    try {
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(2);
      writeBatchedAlignedTsFile(
          sourceTsFile,
          Collections.singletonList(deviceID),
          timeWindows,
          BATCHED_MEASUREMENT_BATCHES,
          emptyMeasurementIdsByWindow,
          nullMeasurementIdsByWindow);

      final List<ChunkData> emittedChunkDataList = splitSourceTsFile(sourceTsFile);
      Assert.assertTrue(emittedChunkDataList.size() > 2);
      for (final ChunkData chunkData : emittedChunkDataList) {
        assertNoDuplicatedChunk(chunkData);
      }

      final Map<TTimePartitionSlot, List<ChunkData>> chunkDataByPartition =
          groupChunkDataByPartition(emittedChunkDataList);
      Assert.assertEquals(2, chunkDataByPartition.size());

      final List<ChunkData> firstPartitionChunkDataList =
          chunkDataByPartition.get(new TTimePartitionSlot(firstPartitionStartTime));
      Assert.assertNotNull(firstPartitionChunkDataList);
      assertBatchedAlignedTarget(
          writeTargetTsFile(targetTsFiles[0], firstPartitionChunkDataList),
          deviceID,
          new long[] {firstPartitionStartTime + 3, crossedTime},
          2,
          nullMeasurements);

      final List<ChunkData> secondPartitionChunkDataList =
          chunkDataByPartition.get(new TTimePartitionSlot(nextPartitionStartTime));
      Assert.assertNotNull(secondPartitionChunkDataList);
      assertBatchedAlignedTarget(
          writeTargetTsFile(targetTsFiles[1], secondPartitionChunkDataList),
          deviceID,
          new long[] {nextPartitionStartTime},
          1,
          nullMeasurements);
    } finally {
      TSFileDescriptor.getInstance()
          .getConfig()
          .setMaxNumberOfPointsInPage(previousMaxNumberOfPointsInPage);
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      for (final File targetTsFile : targetTsFiles) {
        if (targetTsFile.exists()) {
          Assert.assertTrue(targetTsFile.delete());
        }
      }
    }
  }

  /** Every device of a batched TsFile keeps its own flush state. */
  @Test
  public void testSplitBatchedAlignedChunkOfMultipleDevices() throws Exception {
    final File sourceTsFile = new File("split-batched-aligned-multi-device-source.tsfile");
    final List<IDeviceID> deviceIDs =
        Arrays.asList(
            new StringArrayDeviceID("table1", "tagA"), new StringArrayDeviceID("table1", "tagB"));
    final int previousMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    final long firstPartitionStartTime =
        TimePartitionUtils.getTimePartitionSlot(1_700_200_000_000L).getStartTime();
    final long timePartitionInterval = TimePartitionUtils.getTimePartitionInterval();
    final List<List<Long>> timeWindows =
        Arrays.asList(
            Arrays.asList(firstPartitionStartTime, firstPartitionStartTime + 1),
            Arrays.asList(
                firstPartitionStartTime + timePartitionInterval,
                firstPartitionStartTime + timePartitionInterval + 1));
    final File[] targetTsFiles = {
      new File("split-batched-aligned-multi-device-target-0.tsfile"),
      new File("split-batched-aligned-multi-device-target-1.tsfile")
    };
    try {
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(2);
      writeBatchedAlignedTsFile(
          sourceTsFile,
          deviceIDs,
          timeWindows,
          BATCHED_MEASUREMENT_BATCHES,
          Collections.emptyMap(),
          Collections.emptyMap());

      final List<ChunkData> emittedChunkDataList = splitSourceTsFile(sourceTsFile);
      for (final ChunkData chunkData : emittedChunkDataList) {
        assertNoDuplicatedChunk(chunkData);
      }

      final Map<TTimePartitionSlot, List<ChunkData>> chunkDataByPartition =
          groupChunkDataByPartition(emittedChunkDataList);
      Assert.assertEquals(2, chunkDataByPartition.size());

      final List<ChunkData> firstPartitionChunkDataList =
          chunkDataByPartition.get(new TTimePartitionSlot(firstPartitionStartTime));
      Assert.assertNotNull(firstPartitionChunkDataList);
      final File firstTargetTsFile =
          writeTargetTsFile(targetTsFiles[0], firstPartitionChunkDataList);
      final List<ChunkData> secondPartitionChunkDataList =
          chunkDataByPartition.get(
              new TTimePartitionSlot(firstPartitionStartTime + timePartitionInterval));
      Assert.assertNotNull(secondPartitionChunkDataList);
      final File secondTargetTsFile =
          writeTargetTsFile(targetTsFiles[1], secondPartitionChunkDataList);

      for (final IDeviceID deviceID : deviceIDs) {
        assertBatchedAlignedTarget(
            firstTargetTsFile,
            deviceID,
            new long[] {firstPartitionStartTime, firstPartitionStartTime + 1},
            1,
            Collections.emptySet());
        assertBatchedAlignedTarget(
            secondTargetTsFile,
            deviceID,
            new long[] {
              firstPartitionStartTime + timePartitionInterval,
              firstPartitionStartTime + timePartitionInterval + 1
            },
            1,
            Collections.emptySet());
      }
    } finally {
      TSFileDescriptor.getInstance()
          .getConfig()
          .setMaxNumberOfPointsInPage(previousMaxNumberOfPointsInPage);
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      for (final File targetTsFile : targetTsFiles) {
        if (targetTsFile.exists()) {
          Assert.assertTrue(targetTsFile.delete());
        }
      }
    }
  }

  private void writeBatchedAlignedTsFile(
      final File tsFile,
      final List<IDeviceID> deviceIDs,
      final List<List<Long>> timeWindows,
      final List<List<String>> measurementBatches,
      final Map<Integer, Set<String>> emptyMeasurementIdsByWindow,
      final Map<Integer, Set<String>> nullMeasurementIdsByWindow)
      throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }
    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(createBatchedSchema());
      for (final IDeviceID deviceID : deviceIDs) {
        writer.startChunkGroup(deviceID);
        for (int batchIndex = 0; batchIndex < measurementBatches.size(); batchIndex++) {
          final List<String> measurementBatch = measurementBatches.get(batchIndex);
          for (int windowIndex = 0; windowIndex < timeWindows.size(); windowIndex++) {
            final List<Long> timeWindow = timeWindows.get(windowIndex);
            final Set<String> emptyMeasurements =
                emptyMeasurementIdsByWindow.getOrDefault(windowIndex, Collections.emptySet());
            final Set<String> nullMeasurements =
                nullMeasurementIdsByWindow.getOrDefault(windowIndex, Collections.emptySet());
            if (batchIndex == 0) {
              // The first batch carries the time chunk of the window, the following batches only
              // append their value chunks behind it, exactly like the batched compaction writer.
              final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
              for (final String measurement : measurementBatch) {
                if (!emptyMeasurements.contains(measurement)) {
                  measurementSchemas.add(new MeasurementSchema(measurement, TSDataType.INT64));
                }
              }
              final AlignedChunkWriterImpl alignedChunkWriter =
                  new AlignedChunkWriterImpl(measurementSchemas);
              for (final long time : timeWindow) {
                alignedChunkWriter.getTimeChunkWriter().write(time);
              }
              int valueIndex = 0;
              for (final String measurement : measurementBatch) {
                if (emptyMeasurements.contains(measurement)) {
                  continue;
                }
                final ValueChunkWriter valueChunkWriter =
                    alignedChunkWriter.getValueChunkWriterByIndex(valueIndex++);
                for (final long time : timeWindow) {
                  valueChunkWriter.write(
                      time,
                      batchedValue(measurement, time),
                      nullMeasurements.contains(measurement));
                }
              }
              alignedChunkWriter.writeToFileWriter(writer);
            } else {
              for (final String measurement : measurementBatch) {
                if (emptyMeasurements.contains(measurement)) {
                  continue;
                }
                final ValueChunkWriter valueChunkWriter =
                    new ValueChunkWriter(
                        measurement,
                        CompressionType.LZ4,
                        TSDataType.INT64,
                        TSEncoding.PLAIN,
                        new PlainEncoder(TSDataType.INT64, 8));
                for (final long time : timeWindow) {
                  valueChunkWriter.write(
                      time,
                      batchedValue(measurement, time),
                      nullMeasurements.contains(measurement));
                }
                valueChunkWriter.writeToFileWriter(writer);
              }
            }
            for (final String measurement : measurementBatch) {
              if (emptyMeasurements.contains(measurement)) {
                writer.writeEmptyValueChunk(
                    measurement,
                    CompressionType.LZ4,
                    TSDataType.INT64,
                    TSEncoding.PLAIN,
                    Statistics.getStatsByType(TSDataType.INT64));
              }
            }
          }
        }
        writer.endChunkGroup();
      }
      writer.endFile();
    }
  }

  private static long batchedValue(final String measurement, final long time) {
    return (BATCHED_MEASUREMENTS.indexOf(measurement) + 1) * 1_000_000_000L + time;
  }

  private List<ChunkData> splitSourceTsFile(final File sourceTsFile) throws Exception {
    final List<ChunkData> emittedChunkDataList = new ArrayList<>();
    final TsFileSplitter splitter =
        new TsFileSplitter(
            sourceTsFile,
            tsFileData -> {
              if (tsFileData instanceof ChunkData) {
                emittedChunkDataList.add((ChunkData) tsFileData);
              }
              return true;
            });
    splitter.splitTsFileByDataPartition();
    return emittedChunkDataList;
  }

  private Map<TTimePartitionSlot, List<ChunkData>> groupChunkDataByPartition(
      final List<ChunkData> emittedChunkDataList) {
    final Map<TTimePartitionSlot, List<ChunkData>> chunkDataByPartition = new LinkedHashMap<>();
    for (final ChunkData chunkData : emittedChunkDataList) {
      chunkDataByPartition
          .computeIfAbsent(chunkData.getTimePartitionSlot(), slot -> new ArrayList<>())
          .add(chunkData);
    }
    return chunkDataByPartition;
  }

  private void assertNoDuplicatedChunk(final ChunkData chunkData) {
    final Set<String> measurementIds = new HashSet<>();
    for (final Chunk chunk : chunkData.getChunks()) {
      final String measurementId = chunk.getHeader().getMeasurementID();
      Assert.assertTrue(
          "Duplicated chunk of measurement "
              + measurementId
              + " in one emitted ChunkData, the chunk data was flushed without being reset",
          measurementIds.add(measurementId.isEmpty() ? "<time>" : measurementId));
    }
  }

  private File writeTargetTsFile(final File targetTsFile, final List<ChunkData> chunkDataList)
      throws Exception {
    if (targetTsFile.exists()) {
      Assert.assertTrue(targetTsFile.delete());
    }
    try (final TsFileIOWriter writer = new TsFileIOWriter(targetTsFile)) {
      writer.setSchema(createBatchedSchema());
      IDeviceID currentDeviceID = null;
      for (final ChunkData chunkData : chunkDataList) {
        if (!Objects.equals(currentDeviceID, chunkData.getDevice())) {
          if (Objects.nonNull(currentDeviceID)) {
            writer.endChunkGroup();
          }
          writer.startChunkGroup(chunkData.getDevice());
          currentDeviceID = chunkData.getDevice();
        }
        writeSerializedChunkDataToWriter(chunkData, writer);
      }
      if (Objects.nonNull(currentDeviceID)) {
        writer.endChunkGroup();
      }
      writer.endFile();
    }
    return targetTsFile;
  }

  private void assertBatchedAlignedTarget(
      final File targetTsFile,
      final IDeviceID deviceID,
      final long[] expectedTimes,
      final int expectedTimeChunkCount,
      final Set<String> nullMeasurements)
      throws Exception {
    try (final TsFileSequenceReader reader =
        new TsFileSequenceReader(targetTsFile.getAbsolutePath())) {
      final List<AbstractAlignedChunkMetadata> chunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID, false);
      Assert.assertEquals(expectedTimeChunkCount, chunkMetadataList.size());
      long totalTimePointCount = 0;
      for (final AbstractAlignedChunkMetadata chunkMetadata : chunkMetadataList) {
        final long timePointCount = chunkMetadata.getTimeChunkMetadata().getStatistics().getCount();
        totalTimePointCount += timePointCount;
        final List<IChunkMetadata> valueChunkMetadataList =
            chunkMetadata.getValueChunkMetadataList();
        Assert.assertEquals(BATCHED_MEASUREMENTS.size(), valueChunkMetadataList.size());
        for (int i = 0; i < BATCHED_MEASUREMENTS.size(); i++) {
          final String measurement = BATCHED_MEASUREMENTS.get(i);
          final IChunkMetadata valueChunkMetadata = valueChunkMetadataList.get(i);
          if (nullMeasurements.contains(measurement)) {
            // A chunk without any non-null point is reported as null by the metadata reader.
            Assert.assertTrue(
                valueChunkMetadata == null || valueChunkMetadata.getStatistics().getCount() == 0);
          } else {
            Assert.assertNotNull(valueChunkMetadata);
            Assert.assertEquals(measurement, valueChunkMetadata.getMeasurementUid());
            Assert.assertEquals(timePointCount, valueChunkMetadata.getStatistics().getCount());
          }
        }
      }
      Assert.assertEquals(expectedTimes.length, totalTimePointCount);
    }
    try (final TsFileReader reader = new TsFileReader(targetTsFile)) {
      final List<Path> paths = new ArrayList<>();
      for (final String measurement : BATCHED_MEASUREMENTS) {
        paths.add(new Path(deviceID, measurement, true));
      }
      final QueryDataSet dataSet = reader.query(QueryExpression.create(paths, null));
      int rowIndex = 0;
      while (dataSet.hasNext()) {
        final RowRecord row = dataSet.next();
        Assert.assertTrue(rowIndex < expectedTimes.length);
        Assert.assertEquals(expectedTimes[rowIndex], row.getTimestamp());
        for (int i = 0; i < BATCHED_MEASUREMENTS.size(); i++) {
          final String measurement = BATCHED_MEASUREMENTS.get(i);
          final Field field = row.getField(i);
          if (nullMeasurements.contains(measurement)) {
            Assert.assertTrue(field == null || field.getDataType() == null);
          } else {
            Assert.assertNotNull(field);
            Assert.assertEquals(batchedValue(measurement, row.getTimestamp()), field.getLongV());
          }
        }
        rowIndex++;
      }
      Assert.assertEquals(expectedTimes.length, rowIndex);
    }
  }

  private Schema createBatchedSchema() {
    final List<IMeasurementSchema> tableSchemaList = new ArrayList<>();
    final List<ColumnCategory> columnCategoryList = new ArrayList<>();
    tableSchemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
    columnCategoryList.add(ColumnCategory.TAG);
    for (final String measurement : BATCHED_MEASUREMENTS) {
      tableSchemaList.add(new MeasurementSchema(measurement, TSDataType.INT64));
      columnCategoryList.add(ColumnCategory.FIELD);
    }

    final Schema schema = new Schema();
    schema.registerTableSchema(new TableSchema("table1", tableSchemaList, columnCategoryList));
    return schema;
  }

  private void writeTableTsFileWithTimeOnlyChunk(
      final File tsFile, final IDeviceID deviceID, final long... times) throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }

    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(createSchema());
      writer.startChunkGroup(deviceID);

      final AlignedChunkWriterImpl chunkWriter =
          new AlignedChunkWriterImpl(Collections.emptyList());
      for (final long time : times) {
        chunkWriter.write(time);
      }
      chunkWriter.writeToFileWriter(writer);

      writer.endChunkGroup();
      writer.endFile();
    }
  }

  private Schema createSchema() {
    final List<IMeasurementSchema> tableSchemaList =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("s1", TSDataType.INT64));
    final List<ColumnCategory> columnCategoryList =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);

    final Schema schema = new Schema();
    schema.registerTableSchema(new TableSchema("table1", tableSchemaList, columnCategoryList));
    return schema;
  }

  private void writeSerializedChunkDataToWriter(
      final ChunkData chunkData, final TsFileIOWriter writer) throws Exception {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      chunkData.serialize(dataOutputStream);
    }
    ((ChunkData)
            TsFileData.deserialize(new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
        .writeToFileWriter(writer);
  }
}
