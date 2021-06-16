/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction;

import java.io.IOException;
import org.apache.iotdb.db.engine.storagegroup.FakedTsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CompactionSchedulerTest {

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    CompactionTaskManager.getInstance().start();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    CompactionTaskManager.getInstance().stop();
  }

  @Test
  public void testFileSelecter() {
    TsFileResourceList tsFileResources = new TsFileResourceList();
    for (int i = 0; i < 3; i++) {
      tsFileResources.add(new FakedTsFileResource(100));
    }
    tsFileResources.add(new FakedTsFileResource(100, false, true));
    for (int i = 0; i < 2; i++) {
      tsFileResources.add(new FakedTsFileResource(100));
    }
    tsFileResources.add(new FakedTsFileResource(100));
    tsFileResources.add(new FakedTsFileResource(100));

    CompactionScheduler.tryToSubmitInnerSpaceCompactionTask("testSG", 0L, tsFileResources, true,
        FakedInnerSpaceCompactionTask.class);
    while (CompactionScheduler.getCnt() != 0) {
      //
    }
    Assert.assertEquals(5, tsFileResources.size());
  }
}
