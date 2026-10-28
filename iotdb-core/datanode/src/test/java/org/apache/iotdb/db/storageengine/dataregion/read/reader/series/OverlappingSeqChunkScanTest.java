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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.series;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.page.PageWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createChunkWriter;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeNonAlignedChunk;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeNonAlignedPoint;

/**
 * Reproduces the mis-ordering caused by overlapping <b>sequence</b> pages.
 *
 * <p>{@link SeriesScanUtil#unpackAllOverlappedChunkMetadataToPageReaders} expands the pages of
 * every overlapping seq chunk, and the consuming side ({@code getFirstPageReaderFromCachedReaders}
 * / {@code firstPageOverlapped}) assumes the page list is globally ordered in scan order and only
 * inspects its head. When two seq files overlap, the pages of the later file are unpacked after the
 * pages of the earlier one, so a page that starts earlier than the head stays invisible: rows are
 * emitted twice (once from each file) and out of time order.
 *
 * <p>Overlapping seq files cannot be produced by the ordinary write path (out-of-order points go to
 * the unseq list), but they do occur in practice when the write path is fed an unsorted tablet, and
 * from historical overlap repaired by {@code TsFileOverlapValidationAndRepairTool}, region
 * migration and file loading.
 */
public class OverlappingSeqChunkScanTest {

  private static final String DATABASE = "root.sg_overlap_seq";
  private static final IDeviceID DEVICE =
      IDeviceID.Factory.DEFAULT_FACTORY.create(DATABASE + ".d1");
  private static final String SERIES_PATH = DEVICE + ".s1";

  private final List<TsFileResource> seqResources = new ArrayList<>();

  @After
  public void tearDown() throws IOException {
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    for (TsFileResource resource : seqResources) {
      if (resource.getTsFile().exists()) {
        resource.remove();
      }
    }
    seqResources.clear();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    EnvironmentUtils.cleanAllDir();
  }

  /**
   * The second file's page starts inside the second page of the first file and ends before the
   * first file's last page, so it only overlaps pages that were already returned.
   *
   * <pre>
   *   f1 : [10,19] [20,29] [30,39]
   *   f2 :              [25,27]
   * </pre>
   */
  @Test
  public void testOverlapNestedInsideEarlierPage() throws Exception {
    writeSeqFileFromRanges(101, pageRange(10, 19, 20, 29, 30, 39), 10, 39);
    writeSeqFileFromRanges(102, pageRange(25, 27), 25, 27);

    Assert.assertEquals(range(10, 39), scanAllTimestamps(Ordering.ASC));
    Assert.assertEquals(reverseRange(10, 39), scanAllTimestamps(Ordering.DESC));
  }

  /**
   * The second file holds a single page whose range spans beyond the whole first file, which is the
   * shape produced by an out-of-order tablet such as {@code [100, 15]}.
   *
   * <pre>
   *   f1 : [10,20] [21,30] [31,39]
   *   f2 :    [15 ................ 100]
   * </pre>
   */
  @Test
  public void testOverlapSpanningBeyondEarlierFile() throws Exception {
    writeSeqFileFromRanges(101, pageRange(10, 20, 21, 30, 31, 39), 10, 39);
    writeSeqFileFromTimestamps(102, Collections.singletonList(Arrays.asList(15L, 100L)), 15, 100);

    List<Long> expected = range(10, 39);
    expected.add(100L);
    Assert.assertEquals(expected, scanAllTimestamps(Ordering.ASC));

    List<Long> expectedDesc = new ArrayList<>();
    expectedDesc.add(100L);
    expectedDesc.addAll(reverseRange(10, 39));
    Assert.assertEquals(expectedDesc, scanAllTimestamps(Ordering.DESC));
  }

  /**
   * The overlap is fully contained in the first page, so the duplicated timestamps must be returned
   * exactly once and in order.
   *
   * <pre>
   *   f1 : [10,19] [20,29]
   *   f2 : [12,14]
   * </pre>
   */
  @Test
  public void testOverlappingTimestampsReturnedOnce() throws Exception {
    writeSeqFileFromRanges(101, pageRange(10, 19, 20, 29), 10, 29);
    writeSeqFileFromTimestamps(
        102, Collections.singletonList(Arrays.asList(12L, 13L, 14L)), 12, 14);

    Assert.assertEquals(range(10, 29), scanAllTimestamps(Ordering.ASC));
  }

  private List<Long> scanAllTimestamps(Ordering ordering) throws Exception {
    MeasurementPath scanPath = new MeasurementPath(SERIES_PATH, TSDataType.INT32);
    SeriesScanOptions.Builder builder = new SeriesScanOptions.Builder();
    builder.withAllSensors(Collections.singleton(scanPath.getMeasurement()));
    SeriesScanUtil scanUtil =
        new SeriesScanUtil(
            IFullPath.convertToIFullPath(scanPath),
            ordering,
            builder.build(),
            EnvironmentUtils.TEST_QUERY_FI_CONTEXT);
    scanUtil.initQueryDataSource(
        new QueryDataSource(new ArrayList<>(seqResources), Collections.emptyList()));

    List<Long> timestamps = new ArrayList<>();
    for (int guard = 0; guard < 10_000; guard++) {
      if (scanUtil.hasNextPage()) {
        TsBlock block = scanUtil.nextPage();
        if (block != null && !block.isEmpty()) {
          for (int i = 0; i < block.getPositionCount(); i++) {
            timestamps.add(block.getTimeByIndex(i));
          }
        }
        continue;
      }
      Optional<Boolean> hasNextChunk = scanUtil.hasNextChunk();
      if (!hasNextChunk.isPresent() || hasNextChunk.get()) {
        continue;
      }
      Optional<Boolean> hasNextFile = scanUtil.hasNextFile();
      if (!hasNextFile.isPresent() || hasNextFile.get()) {
        continue;
      }
      return timestamps;
    }
    Assert.fail("scan did not terminate");
    return timestamps;
  }

  private static List<Long> range(long from, long to) {
    List<Long> times = new ArrayList<>();
    for (long t = from; t <= to; t++) {
      times.add(t);
    }
    return times;
  }

  private static List<Long> reverseRange(long from, long to) {
    List<Long> times = new ArrayList<>();
    for (long t = to; t >= from; t--) {
      times.add(t);
    }
    return times;
  }

  private static List<TimeRange> pageRange(long... bounds) {
    List<TimeRange> pages = new ArrayList<>();
    for (int i = 0; i < bounds.length; i += 2) {
      pages.add(new TimeRange(bounds[i], bounds[i + 1]));
    }
    return pages;
  }

  private void writeSeqFileFromRanges(
      int fileIndex, List<TimeRange> pages, long startTime, long endTime)
      throws IOException, IllegalPathException {
    writeSeqFile(
        fileIndex,
        (chunkWriters, fileWriter) -> {
          for (IChunkWriter writer : chunkWriters) {
            writeNonAlignedChunk((ChunkWriterImpl) writer, fileWriter, pages, true);
          }
        },
        startTime,
        endTime);
  }

  private void writeSeqFileFromTimestamps(
      int fileIndex, List<List<Long>> pages, long startTime, long endTime)
      throws IOException, IllegalPathException {
    writeSeqFile(
        fileIndex,
        (chunkWriters, fileWriter) -> {
          for (IChunkWriter writer : chunkWriters) {
            ChunkWriterImpl chunk = (ChunkWriterImpl) writer;
            PageWriter pageWriter = chunk.getPageWriter();
            for (List<Long> page : pages) {
              for (long timestamp : page) {
                writeNonAlignedPoint(pageWriter, timestamp, true);
              }
              chunk.sealCurrentPage();
            }
            chunk.writeToFileWriter(fileWriter);
          }
        },
        startTime,
        endTime);
  }

  private interface ChunkBody {
    void write(List<IChunkWriter> chunkWriters, TsFileIOWriter fileWriter) throws IOException;
  }

  private void writeSeqFile(int fileIndex, ChunkBody body, long startTime, long endTime)
      throws IOException, IllegalPathException {
    File file = new File(TestConstant.getTestTsFilePath(DATABASE, 0, 0, fileIndex));
    TsFileResource resource = new TsFileResource(file);
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }
    try (TsFileIOWriter writer = new TsFileIOWriter(resource.getTsFile())) {
      writer.startChunkGroup(DEVICE);
      List<PartialPath> writtenPaths = Collections.singletonList(new PartialPath(SERIES_PATH));
      body.write(
          createChunkWriter(
              writtenPaths,
              Collections.singletonList(TSDataType.INT32),
              Collections.singletonList(TSEncoding.PLAIN),
              Collections.singletonList(CompressionType.UNCOMPRESSED),
              false),
          writer);
      writer.endChunkGroup();
      resource.updateStartTime(DEVICE, startTime);
      resource.updateEndTime(DEVICE, endTime);
      writer.endFile();
    }
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    seqResources.add(resource);
  }
}
