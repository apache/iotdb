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

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.COMPACTION_LOG_NAME;
import static org.junit.Assert.assertFalse;

public class InnerCompactionLogTest extends InnerCompactionTest {

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
  public void testCompactionLog() {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    while (CompactionScheduler.isPartitionCompacting(COMPACTION_TEST_SG, 0)) {
      // wait
    }
    File logFile =
        FSFactoryProducer.getFSFactory()
            .getFile(tempSGDir.getPath(), COMPACTION_TEST_SG + COMPACTION_LOG_NAME);
    assertFalse(logFile.exists());
  }
}
