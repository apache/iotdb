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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

public class FileMetricsTest {

  private static final String DATABASE = "test_file_metrics";
  private static final String REGION_WITH_TSFILE = "100001";
  private static final String REGION_WITH_OBJECT_ONLY = "100002";
  private static final String TSFILE_NAME = "1-1-0-0.tsfile";
  private static final long TSFILE_SIZE = 100L;
  private static final long OBJECT_FILE_SIZE = 40L;
  private static final long OBJECT_ONLY_SIZE = 60L;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void tearDown() {
    FileMetrics.getInstance().deleteRegion(DATABASE, REGION_WITH_TSFILE);
    FileMetrics.getInstance().deleteRegion(DATABASE, REGION_WITH_OBJECT_ONLY);
  }

  @Test
  public void testRegionSizeMaps() throws IOException {
    File regionDir = temporaryFolder.newFolder(DATABASE, REGION_WITH_TSFILE, "0");
    File tsFile = new File(regionDir, TSFILE_NAME);
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(tsFile, "rw")) {
      randomAccessFile.setLength(TSFILE_SIZE);
    }
    TsFileResource tsFileResource = new TsFileResource(tsFile);
    tsFileResource.setSeq(true);
    FileMetrics.getInstance().addTsFile(tsFileResource);
    FileMetrics.getInstance()
        .increaseObjectFileSize(DATABASE, REGION_WITH_TSFILE, OBJECT_FILE_SIZE);
    FileMetrics.getInstance()
        .increaseObjectFileSize(DATABASE, REGION_WITH_OBJECT_ONLY, OBJECT_ONLY_SIZE);

    Map<Integer, Long> regionSizeMap = FileMetrics.getInstance().getRegionSizeMap();
    Map<Integer, Long> regionObjectSizeMap = FileMetrics.getInstance().getRegionObjectSizeMap();
    Map<Integer, Long> regionTotalSizeMap = FileMetrics.getInstance().getRegionTotalSizeMap();

    Assert.assertEquals(
        Long.valueOf(TSFILE_SIZE), regionSizeMap.get(Integer.parseInt(REGION_WITH_TSFILE)));
    Assert.assertNull(regionSizeMap.get(Integer.parseInt(REGION_WITH_OBJECT_ONLY)));
    Assert.assertEquals(
        Long.valueOf(OBJECT_FILE_SIZE),
        regionObjectSizeMap.get(Integer.parseInt(REGION_WITH_TSFILE)));
    Assert.assertEquals(
        Long.valueOf(OBJECT_ONLY_SIZE),
        regionObjectSizeMap.get(Integer.parseInt(REGION_WITH_OBJECT_ONLY)));
    Assert.assertEquals(
        Long.valueOf(TSFILE_SIZE + OBJECT_FILE_SIZE),
        regionTotalSizeMap.get(Integer.parseInt(REGION_WITH_TSFILE)));
    Assert.assertEquals(
        Long.valueOf(OBJECT_ONLY_SIZE),
        regionTotalSizeMap.get(Integer.parseInt(REGION_WITH_OBJECT_ONLY)));
  }
}
