/**
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
package org.apache.iotdb.db.engine.filenode;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileNodeProcessorStoreTest {

  private boolean isOverflowed;
  private Map<String, Long> lastUpdateTimeMap;
  private TsFileResource emptyTsFileResource;
  private List<TsFileResource> newFileNodes;
  private int numOfMergeFile;
  private FileNodeProcessorStatus fileNodeProcessorStatus;

  private FileNodeProcessorStore fileNodeProcessorStore;

  @Before
  public void setUp() throws Exception {
    isOverflowed = true;
    lastUpdateTimeMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      lastUpdateTimeMap.put("d" + i, (long) i);
    }
    emptyTsFileResource = TsFileResourceTest.constructTsfileResource();
    newFileNodes = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      newFileNodes.add(TsFileResourceTest.constructTsfileResource());
    }
    numOfMergeFile = 5;
    fileNodeProcessorStatus = FileNodeProcessorStatus.MERGING_WRITE;
    fileNodeProcessorStore = new FileNodeProcessorStore(isOverflowed, lastUpdateTimeMap,
        emptyTsFileResource, newFileNodes, fileNodeProcessorStatus, numOfMergeFile);
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testSerDeialize() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    fileNodeProcessorStore.serialize(outputStream);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    FileNodeProcessorStore deFileNodeProcessorStore = FileNodeProcessorStore
        .deSerialize(inputStream);

    assertEquals(fileNodeProcessorStore.getLastUpdateTimeMap(),
        deFileNodeProcessorStore.getLastUpdateTimeMap());
    assertEquals(fileNodeProcessorStore.getNumOfMergeFile(),
        deFileNodeProcessorStore.getNumOfMergeFile());
    assertEquals(fileNodeProcessorStore.getFileNodeProcessorStatus(),
        deFileNodeProcessorStore.getFileNodeProcessorStatus());
    TsFileResourceTest.assertTsfileRecource(fileNodeProcessorStore.getEmptyTsFileResource(),
        deFileNodeProcessorStore.getEmptyTsFileResource());
    assertEquals(fileNodeProcessorStore.getNewFileNodes().size(),
        deFileNodeProcessorStore.getNewFileNodes().size());
    for (int i = 0; i < fileNodeProcessorStore.getNewFileNodes().size(); i++) {
      TsFileResourceTest.assertTsfileRecource(fileNodeProcessorStore.getNewFileNodes().get(i),
          deFileNodeProcessorStore.getNewFileNodes().get(i));
    }
  }

}