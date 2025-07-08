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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CompactionDeleteObjectFileTest extends AbstractCompactionTest {
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
  public void test1() throws IOException {
    File dir = new File("/Users/shuww/Downloads/0708/1_副本");
    List<TsFileResource> resources = new ArrayList<>();
    for (File file : dir.listFiles()) {
      if (!file.getName().endsWith(".tsfile")) {
        continue;
      }
      TsFileResource resource = new TsFileResource(file);
      try (ModificationFile modificationFile = resource.getExclusiveModFile()) {
        modificationFile.write(
            new TableDeletionEntry(
                new DeletionPredicate(
                    "tsfile_table",
                    new IDPredicate.FullExactMatch(
                        new StringArrayDeviceID(new String[] {"tsfile_table", "1", "5", "3"})),
                    Arrays.asList("file")),
                new TimeRange(1, 20)));
      }
      resource.deserialize();
      resources.add(resource);
    }

    //    InnerSpaceCompactionTask task =
    //        new InnerSpaceCompactionTask(
    //            0, tsFileManager, resources, true, new ReadChunkCompactionPerformer(), 0);
    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            resources,
            Collections.emptyList(),
            true,
            new FastCompactionPerformer(false),
            0);
    task.start();
  }
}
