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

import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.WritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Covers the streaming mem chunk branch of {@code SeriesScanUtil.unpackOneFakeMemChunkMetaData},
 * which unpacks the fake pages of a mem chunk into the sequence page readers. That branch is the
 * other caller of the scan-order insertion used for overlapping sequence pages, and a mem chunk
 * spanning several fake pages also exercises the reversed page iteration of a descending scan.
 *
 * <p>The chunk is wrapped into an unsealed {@link TsFileResource}, the same shape the storage layer
 * hands to a query while a memtable has not been flushed yet.
 */
public class StreamingMemChunkScanTest {

  private static final String DATABASE = "root.sg_mem_chunk_scan";
  private static final IDeviceID DEVICE =
      IDeviceID.Factory.DEFAULT_FACTORY.create(DATABASE + ".d1");
  private static final String SERIES_PATH = DEVICE + ".s1";
  private static final String MEASUREMENT = "s1";
  private static final int POINT_COUNT = 250;

  private int originalMaxNumberOfPointsInPage;

  @Before
  public void setUp() {
    originalMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    // a fake page holds 10 * maxNumberOfPointsInPage points, so this yields more than one page
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10);
  }

  @After
  public void tearDown() {
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxNumberOfPointsInPage(originalMaxNumberOfPointsInPage);
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testMemChunkPagesAreScannedInOrder() throws Exception {
    List<Long> expectedAsc = range(0, POINT_COUNT - 1);
    Assert.assertEquals(expectedAsc, scanAllTimestamps(Ordering.ASC));
    Assert.assertEquals(reverseRange(0, POINT_COUNT - 1), scanAllTimestamps(Ordering.DESC));
  }

  private List<Long> scanAllTimestamps(Ordering ordering) throws Exception {
    MeasurementPath scanPath = new MeasurementPath(SERIES_PATH, TSDataType.INT32);
    IFullPath fullPath = IFullPath.convertToIFullPath(scanPath);
    TsFileResource memChunkResource = createUnsealedResource(fullPath);
    Assert.assertFalse(memChunkResource.isClosed());

    SeriesScanOptions.Builder builder = new SeriesScanOptions.Builder();
    builder.withAllSensors(Collections.singleton(MEASUREMENT));
    SeriesScanUtil scanUtil =
        new SeriesScanUtil(
            fullPath, ordering, builder.build(), EnvironmentUtils.TEST_QUERY_FI_CONTEXT);
    scanUtil.initQueryDataSource(
        new QueryDataSource(Collections.singletonList(memChunkResource), Collections.emptyList()));

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

  private TsFileResource createUnsealedResource(IFullPath fullPath) throws Exception {
    WritableMemChunk memChunk =
        new WritableMemChunk(
            new MeasurementSchema(MEASUREMENT, TSDataType.INT32, TSEncoding.PLAIN));
    for (int i = 0; i < POINT_COUNT; i++) {
      memChunk.writeNonAlignedPoint(i, i);
    }
    Map<TVList, Integer> tvListQueryMap = new HashMap<>();
    for (TVList tvList : memChunk.getSortedList()) {
      tvListQueryMap.put(tvList, tvList.rowCount());
    }
    tvListQueryMap.put(memChunk.getWorkingTVList(), memChunk.getWorkingTVList().rowCount());
    ReadOnlyMemChunk readOnlyMemChunk =
        new ReadOnlyMemChunk(
            new QueryContext(false, false),
            MEASUREMENT,
            TSDataType.INT32,
            TSEncoding.PLAIN,
            tvListQueryMap,
            null,
            null);

    Map<IFullPath, List<ReadOnlyMemChunk>> pathToReadOnlyMemChunkMap = new HashMap<>();
    pathToReadOnlyMemChunkMap.put(fullPath, Collections.singletonList(readOnlyMemChunk));
    Map<IFullPath, List<IChunkMetadata>> pathToChunkMetadataListMap = new HashMap<>();
    pathToChunkMetadataListMap.put(fullPath, Collections.emptyList());

    File file = new File(TestConstant.getTestTsFilePath(DATABASE, 0, 0, 101));
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }
    TsFileResource originResource = new TsFileResource(file);
    originResource.updateStartTime(DEVICE, 0);
    originResource.updateEndTime(DEVICE, POINT_COUNT - 1);
    return new TsFileResource(
        pathToReadOnlyMemChunkMap, pathToChunkMetadataListMap, originResource);
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
}
