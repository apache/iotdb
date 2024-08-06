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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner.sizetiered;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.NewSizeTieredCompactionSelector;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class NewSizeTieredCompactionSelectorTest extends AbstractCompactionTest {
  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(1024 * 1024 * 2);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerCompactionTotalFileSizeThreshold(1024 * 1024 * 10);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void test1() throws IOException, MetadataException, WriteProcessException {
    createFiles(10, 1000, 10, 100, 100, 10, 10, 10, true, true);
    NewSizeTieredCompactionSelector selector =
        new NewSizeTieredCompactionSelector(COMPACTION_TEST_SG, "0", 0, true, tsFileManager);
    List<InnerSpaceCompactionTask> innerSpaceCompactionTasks =
        selector.selectInnerSpaceTask(seqResources);
    for (InnerSpaceCompactionTask innerSpaceCompactionTask : innerSpaceCompactionTasks) {
      System.out.println(innerSpaceCompactionTask.getAllSourceTsFiles());
    }
  }
}
