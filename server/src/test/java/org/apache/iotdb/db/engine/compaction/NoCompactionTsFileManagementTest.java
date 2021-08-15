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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.TsFileManagement.CompactionMergeTask;
import org.apache.iotdb.db.engine.compaction.no.NoCompactionTsFileManagement;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NoCompactionTsFileManagementTest extends LevelCompactionTest {

  File tempSGDir;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  /** just compaction once */
  @Test
  public void testAddRemoveAndIterator() {
    NoCompactionTsFileManagement noCompactionTsFileManagement =
        new NoCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    for (TsFileResource tsFileResource : seqResources) {
      noCompactionTsFileManagement.add(tsFileResource, true);
    }
    noCompactionTsFileManagement.addAll(seqResources, false);
    assertEquals(6, noCompactionTsFileManagement.getTsFileList(true).size());
    assertEquals(6, noCompactionTsFileManagement.getTsFileList(false).size());
    assertEquals(6, noCompactionTsFileManagement.size(true));
    assertEquals(6, noCompactionTsFileManagement.size(false));
    assertTrue(noCompactionTsFileManagement.contains(seqResources.get(0), true));
    assertFalse(
        noCompactionTsFileManagement.contains(
            new TsFileResource(
                new File(
                    TestConstant.BASE_OUTPUT_PATH.concat(
                        10
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 10
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + ".tsfile"))),
            false));
    assertTrue(noCompactionTsFileManagement.contains(seqResources.get(0), false));
    assertFalse(
        noCompactionTsFileManagement.contains(
            new TsFileResource(
                new File(
                    TestConstant.BASE_OUTPUT_PATH.concat(
                        10
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 10
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + ".tsfile"))),
            false));
    assertFalse(noCompactionTsFileManagement.isEmpty(true));
    assertFalse(noCompactionTsFileManagement.isEmpty(false));
    noCompactionTsFileManagement.remove(
        noCompactionTsFileManagement.getTsFileList(true).get(0), true);
    noCompactionTsFileManagement.remove(
        noCompactionTsFileManagement.getTsFileList(false).get(0), false);
    assertEquals(5, noCompactionTsFileManagement.getTsFileList(true).size());
    noCompactionTsFileManagement.removeAll(
        noCompactionTsFileManagement.getTsFileList(false), false);
    assertEquals(0, noCompactionTsFileManagement.getTsFileList(false).size());
    long count = 0;
    Iterator<TsFileResource> iterator = noCompactionTsFileManagement.getIterator(true);
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(5, count);
    noCompactionTsFileManagement.removeAll(noCompactionTsFileManagement.getTsFileList(true), true);
    assertEquals(0, noCompactionTsFileManagement.getTsFileList(true).size());
    assertTrue(noCompactionTsFileManagement.isEmpty(true));
    assertTrue(noCompactionTsFileManagement.isEmpty(false));
    noCompactionTsFileManagement.add(
        new TsFileResource(
            new File(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"))),
        true);
    noCompactionTsFileManagement.add(
        new TsFileResource(
            new File(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"))),
        false);
    noCompactionTsFileManagement.forkCurrentFileList(0);
    noCompactionTsFileManagement.recover();
    CompactionMergeTask compactionMergeTask =
        noCompactionTsFileManagement
        .new CompactionMergeTask(
            (boolean isMergeExecutedInCurrentTask, long timePartitionId) -> {}, 0);
    compactionMergeTask.call();
    assertEquals(1, noCompactionTsFileManagement.size(true));
    assertEquals(1, noCompactionTsFileManagement.size(false));
    noCompactionTsFileManagement.clear();
    assertEquals(0, noCompactionTsFileManagement.size(true));
    assertEquals(0, noCompactionTsFileManagement.size(false));
  }

  @Test
  public void testIteratorRemove() {
    NoCompactionTsFileManagement noCompactionTsFileManagement =
        new NoCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    for (TsFileResource tsFileResource : seqResources) {
      noCompactionTsFileManagement.add(tsFileResource, true);
    }
    noCompactionTsFileManagement.addAll(seqResources, false);
    assertEquals(6, noCompactionTsFileManagement.getTsFileList(true).size());

    Iterator<TsFileResource> tsFileResourceIterator =
        noCompactionTsFileManagement.getIterator(true);
    tsFileResourceIterator.next();
    try {
      tsFileResourceIterator.remove();
    } catch (UnsupportedOperationException e) {
      // pass
    }
    assertEquals(6, noCompactionTsFileManagement.getTsFileList(true).size());

    TsFileResource tsFileResource1 =
        new TsFileResource(
            new File(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 10
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile")));
    TsFileResource tsFileResource2 =
        new TsFileResource(
            new File(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    11
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 11
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile")));
    noCompactionTsFileManagement.add(tsFileResource1, true);
    noCompactionTsFileManagement.add(tsFileResource2, true);
    TsFileResource tsFileResource3 =
        new TsFileResource(
            new File(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    12
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 12
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 2
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile")));
    noCompactionTsFileManagement.add(tsFileResource3, true);
    Iterator<TsFileResource> tsFileResourceIterator2 =
        noCompactionTsFileManagement.getIterator(true);
    int count = 0;
    while (tsFileResourceIterator2.hasNext()) {
      count++;
      tsFileResourceIterator2.next();
    }
    assertEquals(9, count);
  }
}
