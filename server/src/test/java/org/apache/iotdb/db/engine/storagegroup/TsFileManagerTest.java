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

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TsFileManagerTest {

  File tempSGDir;
  private TsFileManager tsFileManager;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    tempSGDir =
        new File(
            TestConstant.BASE_OUTPUT_PATH
                .concat("tempSG")
                .concat(File.separator)
                .concat("0") // vsg
                .concat(File.separator)
                .concat("0")); // time partition
    tempSGDir.mkdirs();
    tsFileManager = new TsFileManager("test", "0", tempSGDir.getAbsolutePath());
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    FileUtils.deleteDirectory(new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG")));
  }

  private TsFileResource generateTsFileResource(int id) {
    File file =
        new File(TsFileNameGenerator.generateNewTsFilePath(tempSGDir.getPath(), id, id, id, id));
    return new TsFileResource(file);
  }

  private TsFileResource generateTsFileResource(long time, long version) throws IOException {
    File file =
        new File(
            TsFileNameGenerator.generateNewTsFilePath(
                TestConstant.BASE_OUTPUT_PATH, time, version, 0, 0));
    file.createNewFile();
    return new TsFileResource(file);
  }

  /** just compaction once */
  @Test
  public void testAddRemoveAndIterator() {
    List<TsFileResource> seqResources = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      TsFileResource resource = generateTsFileResource(i);
      seqResources.add(resource);
    }
    List<TsFileResource> unseqResources = new ArrayList<>();
    for (int i = 6; i < 10; i++) {
      TsFileResource resource = generateTsFileResource(i);
      unseqResources.add(resource);
    }
    for (TsFileResource tsFileResource : seqResources) {
      tsFileManager.add(tsFileResource, true);
    }
    tsFileManager.addAll(unseqResources, false);
    assertEquals(5, tsFileManager.getTsFileList(true).size());
    assertEquals(4, tsFileManager.getTsFileList(false).size());
    assertEquals(5, tsFileManager.size(true));
    assertEquals(4, tsFileManager.size(false));
    assertTrue(tsFileManager.contains(seqResources.get(0), true));
    assertFalse(
        tsFileManager.contains(
            new TsFileResource(
                new File(
                    TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                        .concat(
                            10
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 10
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + ".tsfile"))),
            false));
    assertFalse(
        tsFileManager.contains(
            new TsFileResource(
                new File(
                    TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                        .concat(
                            10
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 10
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + IoTDBConstant.FILE_NAME_SEPARATOR
                                + 0
                                + ".tsfile"))),
            false));
    assertFalse(tsFileManager.isEmpty(true));
    assertFalse(tsFileManager.isEmpty(false));
    tsFileManager.remove(tsFileManager.getTsFileList(true).get(0), true);
    tsFileManager.remove(tsFileManager.getTsFileList(false).get(0), false);
    assertEquals(4, tsFileManager.getTsFileList(true).size());
    tsFileManager.removeAll(tsFileManager.getTsFileList(false), false);
    assertEquals(0, tsFileManager.getTsFileList(false).size());
    long count = 0;
    Iterator<TsFileResource> iterator = tsFileManager.getIterator(true);
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(4, count);
    tsFileManager.removeAll(tsFileManager.getTsFileList(true), true);
    assertEquals(0, tsFileManager.getTsFileList(true).size());
    assertTrue(tsFileManager.isEmpty(true));
    assertTrue(tsFileManager.isEmpty(false));
    tsFileManager.add(
        new TsFileResource(
            new File(
                TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                    .concat(
                        10
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 10
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 10
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + ".tsfile"))),
        true);
    tsFileManager.add(
        new TsFileResource(
            new File(
                TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                    .concat(
                        10
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 10
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 10
                            + IoTDBConstant.FILE_NAME_SEPARATOR
                            + 0
                            + ".tsfile"))),
        false);
    assertEquals(1, tsFileManager.size(true));
    assertEquals(1, tsFileManager.size(false));
    tsFileManager.clear();
    assertEquals(0, tsFileManager.size(true));
    assertEquals(0, tsFileManager.size(false));
  }

  @Test
  public void testIteratorRemove() {
    List<TsFileResource> seqResources = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      TsFileResource resource = generateTsFileResource(i);
      seqResources.add(resource);
    }
    for (TsFileResource tsFileResource : seqResources) {
      tsFileManager.add(tsFileResource, true);
    }
    tsFileManager.addAll(seqResources, false);
    assertEquals(5, tsFileManager.getTsFileList(true).size());

    Iterator<TsFileResource> tsFileResourceIterator = tsFileManager.getIterator(true);
    tsFileResourceIterator.next();
    try {
      tsFileResourceIterator.remove();
    } catch (UnsupportedOperationException e) {
      // pass
    }
    assertEquals(5, tsFileManager.getTsFileList(true).size());

    TsFileResource tsFileResource1 =
        new TsFileResource(
            new File(
                TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                    .concat(
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
    tsFileManager.add(tsFileResource1, true);
    tsFileManager.add(tsFileResource2, true);
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
    tsFileManager.add(tsFileResource3, true);
    Iterator<TsFileResource> tsFileResourceIterator2 = tsFileManager.getIterator(true);
    int count = 0;
    while (tsFileResourceIterator2.hasNext()) {
      count++;
      tsFileResourceIterator2.next();
    }
    assertEquals(8, count);
  }

  /**
   * seq: [1-0-0-0, 2-0-0-0, 6-0-0-0, 6-2-0-0, 10-0-0-0] unseq: [1-1-0-0, 4-0-0-0, 6-1-0-0, 8-0-0-0,
   * 9-0-0-0] add 2 files with time=6 after seq/6-0-0-0
   */
  @Test
  public void testKeepOrderAddAllAndRenameAfter01() throws IOException {
    List<TsFileResource> seqResources = new ArrayList<>();
    seqResources.add(generateTsFileResource(1, 0));
    seqResources.add(generateTsFileResource(2, 0));
    seqResources.add(generateTsFileResource(6, 0));
    seqResources.add(generateTsFileResource(6, 2));
    seqResources.add(generateTsFileResource(10, 0));
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> unseqResources = new ArrayList<>();
    unseqResources.add(generateTsFileResource(1, 1));
    unseqResources.add(generateTsFileResource(4, 0));
    unseqResources.add(generateTsFileResource(6, 1));
    unseqResources.add(generateTsFileResource(8, 0));
    unseqResources.add(generateTsFileResource(9, 0));
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> filesToAdd = new ArrayList<>();
    filesToAdd.add(generateTsFileResource(6, 0));
    filesToAdd.add(generateTsFileResource(6, 1));
    tsFileManager.keepOrderAddAllAndRenameAfter(seqResources.get(2), filesToAdd, true, 0);

    List<TsFileResource> expect = new ArrayList<>();
    expect.add(seqResources.get(2));
    expect.addAll(filesToAdd);
    expect.add(unseqResources.get(2));
    expect.add(seqResources.get(3));
    expect.addAll(filesToAdd);
    List<TsFileResource> result = new ArrayList<>();
    TsFileResourceList seqList = tsFileManager.getSequenceListByTimePartition(0);
    TsFileResourceList unseqList = tsFileManager.getUnsequenceListByTimePartition(0);
    result.add(seqList.get(2));
    result.add(seqList.get(3));
    result.add(seqList.get(4));
    result.add(unseqList.get(2));
    result.add(seqList.get(5));
    for (int i = 0; i < result.size(); i++) {
      assertEquals(expect.get(i), result.get(i));
      assertEquals(6, result.get(i).getCreatedTime());
      assertEquals(i, result.get(i).getVersion());
    }
  }

  /**
   * seq: [1-0-0-0, 2-0-0-0, 6-0-0-0, 6-2-0-0, 10-0-0-0] unseq: [1-1-0-0, 4-0-0-0, 6-1-0-0, 8-0-0-0,
   * 9-0-0-0] add 2 files with time=1 before unseq/1-1-0-0
   */
  @Test
  public void testKeepOrderAddAllAndRenameAfter02() throws IOException {
    List<TsFileResource> seqResources = new ArrayList<>();
    seqResources.add(generateTsFileResource(1, 0));
    seqResources.add(generateTsFileResource(2, 0));
    seqResources.add(generateTsFileResource(6, 0));
    seqResources.add(generateTsFileResource(6, 2));
    seqResources.add(generateTsFileResource(10, 0));
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> unseqResources = new ArrayList<>();
    unseqResources.add(generateTsFileResource(1, 1));
    unseqResources.add(generateTsFileResource(4, 0));
    unseqResources.add(generateTsFileResource(6, 1));
    unseqResources.add(generateTsFileResource(8, 0));
    unseqResources.add(generateTsFileResource(9, 0));
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> filesToAdd = new ArrayList<>();
    filesToAdd.add(generateTsFileResource(1, 0));
    filesToAdd.add(generateTsFileResource(1, 1));
    tsFileManager.keepOrderAddAllAndRenameAfter(unseqResources.get(0), filesToAdd, false, 0);

    List<TsFileResource> expect = new ArrayList<>();
    expect.add(seqResources.get(0));
    expect.add(unseqResources.get(0));
    expect.addAll(filesToAdd);
    List<TsFileResource> result = new ArrayList<>();
    TsFileResourceList seqList = tsFileManager.getSequenceListByTimePartition(0);
    TsFileResourceList unseqList = tsFileManager.getUnsequenceListByTimePartition(0);
    result.add(seqList.get(0));
    result.add(unseqList.get(0));
    result.add(unseqList.get(1));
    result.add(unseqList.get(2));
    for (int i = 0; i < result.size(); i++) {
      assertEquals(expect.get(i), result.get(i));
      assertEquals(1, result.get(i).getCreatedTime());
      assertEquals(i, result.get(i).getVersion());
    }
  }

  /**
   * seq: [1-0-0-0, 2-0-0-0, 6-0-0-0, 6-2-0-0, 10-0-0-0] unseq: [1-1-0-0, 4-0-0-0, 6-1-0-0, 8-0-0-0,
   * 9-0-0-0] add 2 files with time=10 after seq/10-0-0-0
   */
  @Test
  public void testKeepOrderAddAllAndRenameAfter03() throws IOException {
    List<TsFileResource> seqResources = new ArrayList<>();
    seqResources.add(generateTsFileResource(1, 0));
    seqResources.add(generateTsFileResource(2, 0));
    seqResources.add(generateTsFileResource(6, 0));
    seqResources.add(generateTsFileResource(6, 2));
    seqResources.add(generateTsFileResource(10, 0));
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> unseqResources = new ArrayList<>();
    unseqResources.add(generateTsFileResource(1, 1));
    unseqResources.add(generateTsFileResource(4, 0));
    unseqResources.add(generateTsFileResource(6, 1));
    unseqResources.add(generateTsFileResource(8, 0));
    unseqResources.add(generateTsFileResource(9, 0));
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> filesToAdd = new ArrayList<>();
    filesToAdd.add(generateTsFileResource(10, 0));
    filesToAdd.add(generateTsFileResource(10, 1));
    tsFileManager.keepOrderAddAllAndRenameAfter(seqResources.get(4), filesToAdd, true, 0);

    List<TsFileResource> expect = new ArrayList<>();
    expect.add(seqResources.get(4));
    expect.addAll(filesToAdd);
    List<TsFileResource> result = new ArrayList<>();
    TsFileResourceList seqList = tsFileManager.getSequenceListByTimePartition(0);
    result.add(seqList.get(4));
    result.add(seqList.get(5));
    result.add(seqList.get(6));
    for (int i = 0; i < result.size(); i++) {
      assertEquals(expect.get(i), result.get(i));
      assertEquals(10, result.get(i).getCreatedTime());
      assertEquals(i, result.get(i).getVersion());
    }
  }

  /**
   * seq: [1-0-0-0, 2-0-0-0, 6-0-0-0, 6-1-0-0, 10-0-0-0] unseq: [] add 2 files with time=6 to unseq
   */
  @Test
  public void testKeepOrderAddAllAndRenameAfter04() throws IOException {
    List<TsFileResource> seqResources = new ArrayList<>();
    seqResources.add(generateTsFileResource(1, 0));
    seqResources.add(generateTsFileResource(2, 0));
    seqResources.add(generateTsFileResource(6, 0));
    seqResources.add(generateTsFileResource(6, 1));
    seqResources.add(generateTsFileResource(10, 0));
    tsFileManager.addAll(seqResources, true);

    List<TsFileResource> filesToAdd = new ArrayList<>();
    filesToAdd.add(generateTsFileResource(6, 0));
    filesToAdd.add(generateTsFileResource(6, 1));
    tsFileManager.keepOrderAddAllAndRenameAfter(null, filesToAdd, false, 0);

    List<TsFileResource> expect = new ArrayList<>();
    expect.add(seqResources.get(2));
    expect.add(seqResources.get(3));
    expect.addAll(filesToAdd);
    List<TsFileResource> result = new ArrayList<>();
    TsFileResourceList seqList = tsFileManager.getSequenceListByTimePartition(0);
    TsFileResourceList unseqList = tsFileManager.getUnsequenceListByTimePartition(0);
    result.add(seqList.get(2));
    result.add(seqList.get(3));
    result.add(unseqList.get(0));
    result.add(unseqList.get(1));
    for (int i = 0; i < result.size(); i++) {
      assertEquals(expect.get(i), result.get(i));
      assertEquals(6, result.get(i).getCreatedTime());
      assertEquals(i, result.get(i).getVersion());
    }
  }

  /**
   * seq: [1-0-0-0, 2-0-0-0, 6-0-0-0, 6-2-0-0, 10-0-0-0] unseq: [1-1-0-0, 4-0-0-0, 6-1-0-0, 8-0-0-0,
   * 9-0-0-0] add 2 files with time=6 after seq/2-0-0-0
   */
  @Test
  public void testKeepOrderAddAllAndRenameAfter05() throws IOException {
    List<TsFileResource> seqResources = new ArrayList<>();
    seqResources.add(generateTsFileResource(1, 0));
    seqResources.add(generateTsFileResource(2, 0));
    seqResources.add(generateTsFileResource(6, 0));
    seqResources.add(generateTsFileResource(6, 2));
    seqResources.add(generateTsFileResource(10, 0));
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> unseqResources = new ArrayList<>();
    unseqResources.add(generateTsFileResource(1, 1));
    unseqResources.add(generateTsFileResource(4, 0));
    unseqResources.add(generateTsFileResource(6, 1));
    unseqResources.add(generateTsFileResource(8, 0));
    unseqResources.add(generateTsFileResource(9, 0));
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> filesToAdd = new ArrayList<>();
    filesToAdd.add(generateTsFileResource(6, 0));
    filesToAdd.add(generateTsFileResource(6, 1));
    tsFileManager.keepOrderAddAllAndRenameAfter(seqResources.get(1), filesToAdd, true, 0);

    List<TsFileResource> expect = new ArrayList<>();
    expect.addAll(filesToAdd);
    expect.add(seqResources.get(2));
    expect.add(unseqResources.get(2));
    expect.add(seqResources.get(3));
    List<TsFileResource> result = new ArrayList<>();
    TsFileResourceList seqList = tsFileManager.getSequenceListByTimePartition(0);
    TsFileResourceList unseqList = tsFileManager.getUnsequenceListByTimePartition(0);
    result.add(seqList.get(2));
    result.add(seqList.get(3));
    result.add(seqList.get(4));
    result.add(unseqList.get(2));
    result.add(seqList.get(5));
    for (int i = 0; i < result.size(); i++) {
      assertEquals(expect.get(i), result.get(i));
      assertEquals(6, result.get(i).getCreatedTime());
      assertEquals(i, result.get(i).getVersion());
    }
  }

  /**
   * seq: [1-0-0-0, 2-0-0-0, 6-0-0-0, 6-2-0-0, 10-0-0-0] unseq: [1-1-0-0, 4-0-0-0, 6-1-0-0, 8-0-0-0,
   * 9-0-0-0] add 2 files with time=5 after seq/2-0-0-0
   */
  @Test
  public void testKeepOrderAddAllAndRenameAfter06() throws IOException {
    List<TsFileResource> seqResources = new ArrayList<>();
    seqResources.add(generateTsFileResource(1, 0));
    seqResources.add(generateTsFileResource(2, 0));
    seqResources.add(generateTsFileResource(6, 0));
    seqResources.add(generateTsFileResource(6, 2));
    seqResources.add(generateTsFileResource(10, 0));
    tsFileManager.addAll(seqResources, true);
    List<TsFileResource> unseqResources = new ArrayList<>();
    unseqResources.add(generateTsFileResource(1, 1));
    unseqResources.add(generateTsFileResource(4, 0));
    unseqResources.add(generateTsFileResource(6, 1));
    unseqResources.add(generateTsFileResource(8, 0));
    unseqResources.add(generateTsFileResource(9, 0));
    tsFileManager.addAll(unseqResources, false);

    List<TsFileResource> filesToAdd = new ArrayList<>();
    filesToAdd.add(generateTsFileResource(5, 0));
    filesToAdd.add(generateTsFileResource(5, 1));
    tsFileManager.keepOrderAddAllAndRenameAfter(seqResources.get(1), filesToAdd, true, 0);

    List<TsFileResource> expect = new ArrayList<>();
    expect.addAll(filesToAdd);
    List<TsFileResource> result = new ArrayList<>();
    TsFileResourceList seqList = tsFileManager.getSequenceListByTimePartition(0);
    result.add(seqList.get(2));
    result.add(seqList.get(3));
    for (int i = 0; i < result.size(); i++) {
      assertEquals(expect.get(i), result.get(i));
      assertEquals(5, result.get(i).getCreatedTime());
      assertEquals(i, result.get(i).getVersion());
    }
  }
}
