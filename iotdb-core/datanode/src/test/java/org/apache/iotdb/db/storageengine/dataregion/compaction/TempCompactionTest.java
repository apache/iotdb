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

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TempCompactionTest extends AbstractCompactionTest {
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void test1() throws IOException {
    String seqPath = "/Users/shuww/wrong-file/tsfile/sequence";
    String unseqPath = "/Users/shuww/wrong-file/tsfile/unsequence";
    for (File file : new File(seqPath).listFiles()) {
      if (file.getName().endsWith(".resource")) {
        continue;
      }
      TsFileResource resource = new TsFileResource(file);
      resource.setStatusForTest(TsFileResourceStatus.NORMAL);
      tsFileManager.keepOrderInsert(resource, true);
    }
    for (File file : new File(unseqPath).listFiles()) {
      if (file.getName().endsWith(".resource")) {
        continue;
      }
      TsFileResource resource = new TsFileResource(file);
      resource.setStatusForTest(TsFileResourceStatus.NORMAL);
      tsFileManager.keepOrderInsert(resource, false);
    }
    List<TsFileResource> seqList = tsFileManager.getTsFileList(true);
    List<TsFileResource> unseqList = tsFileManager.getTsFileList(false);
    System.out.println();

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            seqList.get(0).getTimePartition(),
            tsFileManager,
            seqList,
            unseqList,
            new FastCompactionPerformer(true),
            0,
            0
        );
    task.start();
  }
}
