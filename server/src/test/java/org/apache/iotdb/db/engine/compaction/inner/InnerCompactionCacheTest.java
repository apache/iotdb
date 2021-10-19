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

package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey;
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.sizetiered.SizeTieredCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InnerCompactionCacheTest extends InnerCompactionTest {

  File tempSGDir;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    tempSGDir = new File(TestConstant.getTestTsFileDir("root.compactionTest", 0, 0));
    if (!tempSGDir.exists()) {
      Assert.assertTrue(tempSGDir.mkdirs());
    }
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
    tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testCompactionChunkCache() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(150000L);
    TsFileResource tsFileResource = seqResources.get(1);
    TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath());
    List<Path> paths = reader.getAllPaths();
    Set<String> allSensors = new TreeSet<>();
    for (Path path : paths) {
      allSensors.add(path.getMeasurement());
    }
    ChunkMetadata firstChunkMetadata = reader.getChunkMetadataList(paths.get(0)).get(0);
    firstChunkMetadata.setFilePath(tsFileResource.getTsFile().getAbsolutePath());
    TimeSeriesMetadataCacheKey firstTimeSeriesMetadataCacheKey =
        new TimeSeriesMetadataCacheKey(
            seqResources.get(1).getTsFilePath(),
            paths.get(0).getDevice(),
            paths.get(0).getMeasurement());

    // add cache
    ChunkCache.getInstance().get(firstChunkMetadata);
    TimeSeriesMetadataCache.getInstance().get(firstTimeSeriesMetadataCacheKey, allSensors);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CompactionScheduler.addPartitionCompaction(COMPACTION_TEST_SG + "-0", 0);
    File targetFile =
        new File(
            TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                .concat(
                    0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
    if (targetFile.exists()) {
      assertTrue(targetFile.delete());
    }
    SizeTieredCompactionTask sizeTieredCompactionTask =
        new SizeTieredCompactionTask(
            COMPACTION_TEST_SG,
            "0",
            0,
            tsFileManager,
            tsFileManager.getSequenceListByTimePartition(0),
            seqResources,
            true,
            CompactionTaskManager.currentTaskNum);
    sizeTieredCompactionTask.call();

    firstChunkMetadata.setFilePath(null);
    try {
      ChunkCache.getInstance().get(firstChunkMetadata);
      fail();
    } catch (NullPointerException e) {
      assertTrue(true);
    }

    try {
      TimeSeriesMetadataCache.getInstance().get(firstTimeSeriesMetadataCacheKey, new TreeSet<>());
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }

    reader.close();
  }
}
