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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipeTsFileEpochProgressIndexKeeperTest {

  private static final String DATA_REGION_ID = "1";
  private static final String TASK_SCOPE_A = "task-scope-a";
  private static final String TASK_SCOPE_B = "task-scope-b";

  private final PipeTsFileEpochProgressIndexKeeper keeper =
      PipeTsFileEpochProgressIndexKeeper.getInstance();

  private File tempDir;
  private final List<String> registeredTsFilePaths = new ArrayList<>();

  @Before
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("pipeTsFileEpochProgressIndexKeeper").toFile();
  }

  @After
  public void tearDown() {
    registeredTsFilePaths.forEach(
        tsFilePath -> {
          keeper.eliminateProgressIndex(DATA_REGION_ID, TASK_SCOPE_A, tsFilePath);
          keeper.eliminateProgressIndex(DATA_REGION_ID, TASK_SCOPE_B, tsFilePath);
        });
    FileUtils.deleteFileOrDirectory(tempDir);
  }

  @Test
  public void testProgressIndexCheckDoesNotLeakAcrossTaskScopes() throws IOException {
    keeper.registerProgressIndex(
        DATA_REGION_ID, TASK_SCOPE_A, createTsFileResource("1-1-0-0.tsfile", 1L));

    final TsFileResource comparedResource = createTsFileResource("1-2-0-0.tsfile", 2L);
    keeper.registerProgressIndex(DATA_REGION_ID, TASK_SCOPE_A, comparedResource);

    Assert.assertTrue(
        keeper.isProgressIndexAfterOrEquals(
            DATA_REGION_ID,
            TASK_SCOPE_A,
            comparedResource.getTsFilePath(),
            new SimpleProgressIndex(1, 2L)));
    Assert.assertFalse(
        keeper.isProgressIndexAfterOrEquals(
            DATA_REGION_ID,
            TASK_SCOPE_B,
            comparedResource.getTsFilePath(),
            new SimpleProgressIndex(1, 2L)));
  }

  @Test
  public void testProgressIndexCheckUsesEqualOrAfterCoverage() throws IOException {
    final ProgressIndex registeredProgressIndex =
        hybridProgressIndex(
            iotProgressIndex(1, 90L), new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 10)));
    keeper.registerProgressIndex(
        DATA_REGION_ID,
        TASK_SCOPE_A,
        createTsFileResource("registered-hybrid.tsfile", registeredProgressIndex));

    Assert.assertFalse(
        keeper.isProgressIndexAfterOrEquals(
            DATA_REGION_ID, TASK_SCOPE_A, "current.tsfile", iotProgressIndex(1, 100L)));

    Assert.assertTrue(
        keeper.isProgressIndexAfterOrEquals(
            DATA_REGION_ID,
            TASK_SCOPE_A,
            "current.tsfile",
            hybridProgressIndex(
                iotProgressIndex(1, 100L),
                new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 10)))));
  }

  private TsFileResource createTsFileResource(final String fileName, final long flushOrderId)
      throws IOException {
    return createTsFileResource(fileName, new SimpleProgressIndex(1, flushOrderId));
  }

  private TsFileResource createTsFileResource(
      final String fileName, final ProgressIndex progressIndex) throws IOException {
    final File file = new File(tempDir, fileName);
    Assert.assertTrue(file.createNewFile());
    registeredTsFilePaths.add(file.getAbsolutePath());

    final TsFileResource resource = new TsFileResource(file);
    resource.updateProgressIndex(progressIndex);
    return resource;
  }

  private ProgressIndex hybridProgressIndex(
      final ProgressIndex firstProgressIndex, final ProgressIndex... progressIndexes) {
    ProgressIndex result = new HybridProgressIndex(firstProgressIndex);
    for (final ProgressIndex progressIndex : progressIndexes) {
      result = result.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex);
    }
    return result;
  }

  private IoTProgressIndex iotProgressIndex(final int peerId, final long searchIndex) {
    final Map<Integer, Long> peerId2SearchIndex = new HashMap<>();
    peerId2SearchIndex.put(peerId, searchIndex);
    return new IoTProgressIndex(peerId2SearchIndex);
  }
}
