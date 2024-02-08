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
package org.apache.iotdb.tsfile.utils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class FilePathUtilsTest {

  private static final String storageGroupName = "root.group_9";
  private static final String virtualSgName = "1";
  private static final long partitionId = 0;
  private static final String tsFileName = "1611199237113-4-0.tsfile";
  private static final String fullPath =
      "target"
          + File.separator
          + storageGroupName
          + File.separator
          + virtualSgName
          + File.separator
          + partitionId
          + File.separator
          + tsFileName;

  private File tsFile;

  @Before
  public void setUp() {
    tsFile = new File(fullPath);
    boolean success = false;
    try {
      FileUtils.forceMkdirParent(tsFile);
      success = tsFile.createNewFile();
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertTrue(success);
  }

  @Test
  public void getLogicalSgNameAndTimePartitionIdPairTest() {
    Pair<String, Long> sgNameAndTimePartitionIdPair =
        FilePathUtils.getLogicalSgNameAndTimePartitionIdPair(tsFile.getAbsolutePath());
    Assert.assertEquals(storageGroupName, sgNameAndTimePartitionIdPair.left);
    Assert.assertEquals(partitionId, sgNameAndTimePartitionIdPair.right.longValue());
  }

  @Test
  public void getLogicalStorageGroupNameTest() {
    String tmpSgName = FilePathUtils.getLogicalStorageGroupName(tsFile.getAbsolutePath());
    Assert.assertEquals(storageGroupName, tmpSgName);
  }

  @Test
  public void getVirtualStorageGroupNameTest() {
    String tmpVirtualSgName = FilePathUtils.getVirtualStorageGroupId(tsFile.getAbsolutePath());
    Assert.assertEquals(virtualSgName, tmpVirtualSgName);
  }

  @Test
  public void getTimePartitionIdTest() {
    long tmpTimePartitionId = FilePathUtils.getTimePartitionId(tsFile.getAbsolutePath());
    Assert.assertEquals(partitionId, tmpTimePartitionId);
  }

  @Test
  public void getTsFilePrefixPathTest() {
    String tsFilePrefixPath = FilePathUtils.getTsFilePrefixPath(tsFile.getAbsolutePath());
    String exceptPrefixPath =
        storageGroupName + File.separator + virtualSgName + File.separator + partitionId;
    Assert.assertEquals(exceptPrefixPath, tsFilePrefixPath);
  }

  @After
  public void tearDown() {
    Assert.assertTrue(tsFile.delete());
  }
}
