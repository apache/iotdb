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
package org.apache.iotdb.db.wal.utils;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class WALFileUtilsTest {
  @Test
  public void binarySearchFileBySearchIndex01() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 0);
    Assert.assertEquals(-1, i);
  }

  @Test
  public void binarySearchFileBySearchIndex02() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 2);
    Assert.assertEquals(0, i);
  }

  @Test
  public void binarySearchFileBySearchIndex03() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 5);
    Assert.assertEquals(0, i);
  }

  @Test
  public void binarySearchFileBySearchIndex04() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 6);
    Assert.assertEquals(2, i);
  }

  @Test
  public void binarySearchFileBySearchIndex05() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 10);
    Assert.assertEquals(2, i);
  }

  @Test
  public void binarySearchFileBySearchIndex06() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 12);
    Assert.assertEquals(2, i);
  }

  @Test
  public void binarySearchFileBySearchIndex07() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, Long.MAX_VALUE);
    Assert.assertEquals(4, i);
  }

  @Test
  public void binarySearchFileBySearchIndex08() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 100, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 200, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 300, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 400, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 10);
    Assert.assertEquals(0, i);
  }

  @Test
  public void binarySearchFileBySearchIndex09() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 100, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 200, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 300, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 400, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 250);
    Assert.assertEquals(2, i);
  }

  @Test
  public void binarySearchFileBySearchIndex10() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(5, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(6, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(7, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(8, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(9, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(10, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(11, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 5);
    Assert.assertEquals(3, i);
  }

  @Test
  public void binarySearchFileBySearchIndex11() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(5, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(6, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(7, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(8, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(9, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(10, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(11, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 6);
    Assert.assertEquals(7, i);
  }

  @Test
  public void binarySearchFileBySearchIndex12() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(5, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(6, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(7, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(8, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(9, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(10, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(11, 12, WALFileStatus.CONTAINS_SEARCH_INDEX))
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 12);
    Assert.assertEquals(7, i);
  }

  @Test
  public void binarySearchFileBySearchIndex13() {
    File[] files =
        new File[] {
          new File(WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(1, 5, WALFileStatus.CONTAINS_NONE_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(2, 5, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(3, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
          new File(WALFileUtils.getLogFileName(4, 12, WALFileStatus.CONTAINS_SEARCH_INDEX)),
        };
    int i = WALFileUtils.binarySearchFileBySearchIndex(files, 5);
    Assert.assertEquals(0, i);

    i = WALFileUtils.binarySearchFileBySearchIndex(files, 6);
    Assert.assertEquals(2, i);

    i = WALFileUtils.binarySearchFileBySearchIndex(files, 13);
    Assert.assertEquals(4, i);

    i = WALFileUtils.binarySearchFileBySearchIndex(files, 100);
    Assert.assertEquals(4, i);

    i = WALFileUtils.binarySearchFileBySearchIndex(files, 0);
    Assert.assertEquals(-1, i);
  }
}
