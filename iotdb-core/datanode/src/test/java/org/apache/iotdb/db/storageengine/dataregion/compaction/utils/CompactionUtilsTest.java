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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionPathUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class CompactionUtilsTest extends AbstractCompactionTest {
  @Override
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @Override
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testCompactionPathUtils() {
    try {
      IDeviceID deviceID = new StringArrayDeviceID(new String[] {"db.table1", null, "tag1"});
      PartialPath path = CompactionPathUtils.getPath(deviceID, "s1");
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testDeleteSourceTsFileUpdatesModMetrics() throws Exception {
    int modFileNumBefore = FileMetrics.getInstance().getModFileNum();
    long modFileSizeBefore = FileMetrics.getInstance().getModFileSize();

    createFiles(2, 1, 1, 10, 0, 0, 10, 10, false, true);

    long totalModFileSize = 0;
    for (int i = 0; i < seqResources.size(); i++) {
      try (ModificationFile modificationFile = seqResources.get(i).getModFileForWrite()) {
        modificationFile.write(
            new TreeDeletionEntry(
                new MeasurementPath(new String[] {COMPACTION_TEST_SG, "d0", "s0"}),
                Long.MIN_VALUE,
                i + 10));
        totalModFileSize += modificationFile.getFileLength();
      }
    }

    Assert.assertEquals(
        modFileNumBefore + seqResources.size(), FileMetrics.getInstance().getModFileNum());
    Assert.assertEquals(
        modFileSizeBefore + totalModFileSize, FileMetrics.getInstance().getModFileSize());

    CompactionUtils.deleteSourceTsFileAndUpdateFileMetrics(new ArrayList<>(seqResources));

    Assert.assertEquals(modFileNumBefore, FileMetrics.getInstance().getModFileNum());
    Assert.assertEquals(modFileSizeBefore, FileMetrics.getInstance().getModFileSize());
  }
}
