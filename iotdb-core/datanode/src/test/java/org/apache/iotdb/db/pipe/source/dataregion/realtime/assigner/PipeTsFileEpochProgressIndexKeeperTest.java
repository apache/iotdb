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

public class PipeTsFileEpochProgressIndexKeeperTest {

  private static final int DATA_REGION_ID = 1;
  private static final String TASK_SCOPE_A = "task-scope-a";
  private static final String TASK_SCOPE_B = "task-scope-b";

  private final PipeTsFileEpochProgressIndexKeeper keeper =
      PipeTsFileEpochProgressIndexKeeper.getInstance();

  private File tempDir;

  @Before
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("pipeTsFileEpochProgressIndexKeeper").toFile();
  }

  @After
  public void tearDown() {
    keeper.clearProgressIndex(DATA_REGION_ID, TASK_SCOPE_A);
    keeper.clearProgressIndex(DATA_REGION_ID, TASK_SCOPE_B);
    FileUtils.deleteFileOrDirectory(tempDir);
  }

  @Test
  public void testDuplicateTsFileLookupIsScopedByTaskInstance() throws IOException {
    final TsFileResource resource = createTsFileResource("shared.tsfile", 1L);

    keeper.registerProgressIndex(DATA_REGION_ID, TASK_SCOPE_A, resource);

    Assert.assertTrue(
        keeper.containsTsFile(DATA_REGION_ID, TASK_SCOPE_A, resource.getTsFilePath()));
    Assert.assertFalse(
        keeper.containsTsFile(DATA_REGION_ID, TASK_SCOPE_B, resource.getTsFilePath()));
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
  public void testClearProgressIndexOnlyRemovesTargetTaskScope() throws IOException {
    final TsFileResource scopeAResource = createTsFileResource("scope-a.tsfile", 1L);
    final TsFileResource scopeBResource = createTsFileResource("scope-b.tsfile", 1L);

    keeper.registerProgressIndex(DATA_REGION_ID, TASK_SCOPE_A, scopeAResource);
    keeper.registerProgressIndex(DATA_REGION_ID, TASK_SCOPE_B, scopeBResource);

    keeper.clearProgressIndex(DATA_REGION_ID, TASK_SCOPE_A);

    Assert.assertFalse(
        keeper.containsTsFile(DATA_REGION_ID, TASK_SCOPE_A, scopeAResource.getTsFilePath()));
    Assert.assertTrue(
        keeper.containsTsFile(DATA_REGION_ID, TASK_SCOPE_B, scopeBResource.getTsFilePath()));
  }

  private TsFileResource createTsFileResource(final String fileName, final long flushOrderId)
      throws IOException {
    final File file = new File(tempDir, fileName);
    Assert.assertTrue(file.createNewFile());

    final TsFileResource resource = new TsFileResource(file);
    resource.updateProgressIndex(new SimpleProgressIndex(1, flushOrderId));
    return resource;
  }
}
