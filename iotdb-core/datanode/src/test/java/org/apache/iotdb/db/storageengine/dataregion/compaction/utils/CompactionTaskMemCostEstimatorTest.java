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

package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.FastCompactionInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.ReadChunkInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class CompactionTaskMemCostEstimatorTest extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testEstimateReadChunkInnerSpaceCompactionTaskMemCost()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(3, 10, 5, 100000, 0, 0, 50, 50, true, true);
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> tsFileList = tsFileManager.getTsFileList(true);
    System.out.println(tsFileList.get(0).getTsFile().getAbsolutePath());
    long cost = new ReadChunkInnerCompactionEstimator().estimateInnerCompactionMemory(tsFileList);
    Assert.assertTrue(cost > 0);
  }

  @Test
  public void testEstimateReadChunkInnerSpaceCompactionTaskMemCost2()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(3, 10, 5, 100, 0, 0, 50, 50, false, true);
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> tsFileList = tsFileManager.getTsFileList(true);
    long cost = new ReadChunkInnerCompactionEstimator().estimateInnerCompactionMemory(tsFileList);
    Assert.assertTrue(cost > 0);
  }

  @Test
  public void testEstimateFastCompactionInnerSpaceCompactionTaskMemCost()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(3, 10, 5, 100000, 0, 0, 50, 50, true, true);
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> tsFileList = tsFileManager.getTsFileList(true);
    System.out.println(tsFileList.get(0).getTsFile().getAbsolutePath());
    long cost =
        new FastCompactionInnerCompactionEstimator().estimateInnerCompactionMemory(tsFileList);
    Assert.assertTrue(cost > 0);
  }

  @Test
  public void testEstimateFastCompactionInnerSpaceCompactionTaskMemCost2()
      throws IOException, MetadataException, WriteProcessException {
    createFiles(3, 10, 5, 100, 0, 0, 50, 50, false, true);
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> tsFileList = tsFileManager.getTsFileList(true);
    long cost =
        new FastCompactionInnerCompactionEstimator().estimateInnerCompactionMemory(tsFileList);
    Assert.assertTrue(cost > 0);
  }
}
