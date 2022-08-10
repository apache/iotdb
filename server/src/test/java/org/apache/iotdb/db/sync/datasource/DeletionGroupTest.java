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
 *
 */

package org.apache.iotdb.db.sync.datasource;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.iotdb.db.sync.datasource.DeletionGroup.DeletedType.FULL_DELETED;
import static org.apache.iotdb.db.sync.datasource.DeletionGroup.DeletedType.NO_DELETED;
import static org.apache.iotdb.db.sync.datasource.DeletionGroup.DeletedType.PARTIAL_DELETED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeletionGroupTest {

  private static DeletionGroup deletionGroup1;
  private static DeletionGroup deletionGroup2; // empty

  @BeforeClass
  public static void prepareData() {
    deletionGroup1 = new DeletionGroup();

    // for item 0
    deletionGroup1.addDelInterval(10, 30);
    deletionGroup1.addDelInterval(20, 40);

    // for item 3
    deletionGroup1.addDelInterval(150, 200);
    deletionGroup1.addDelInterval(150, 200);

    // for item 1
    deletionGroup1.addDelInterval(50, 50);
    deletionGroup1.addDelInterval(50, 50);

    // for item 4
    deletionGroup1.addDelInterval(220, 300);
    deletionGroup1.addDelInterval(250, 290);

    // for item 2
    deletionGroup1.addDelInterval(70, 110);
    deletionGroup1.addDelInterval(70, 80);
    deletionGroup1.addDelInterval(80, 90);
    deletionGroup1.addDelInterval(100, 120);

    deletionGroup2 = new DeletionGroup();
  }

  @Test
  public void testAddDelInterval() {
    boolean hasException = false;
    try {
      deletionGroup1.addDelInterval(10, 5);
    } catch (IllegalArgumentException e) {
      hasException = true;
    }
    assertTrue(hasException);

    TreeMap<Long, Long> delIntervalMap = deletionGroup1.getDelIntervalMap();
    Iterator<Map.Entry<Long, Long>> iter1 = delIntervalMap.entrySet().iterator();
    Map.Entry<Long, Long> entry1 = iter1.next();
    assertEquals(10, entry1.getKey().longValue());
    assertEquals(40, entry1.getValue().longValue());
    entry1 = iter1.next();
    assertEquals(50, entry1.getKey().longValue());
    assertEquals(50, entry1.getValue().longValue());
    entry1 = iter1.next();
    assertEquals(70, entry1.getKey().longValue());
    assertEquals(120, entry1.getValue().longValue());
    entry1 = iter1.next();
    assertEquals(150, entry1.getKey().longValue());
    assertEquals(200, entry1.getValue().longValue());
    entry1 = iter1.next();
    assertEquals(220, entry1.getKey().longValue());
    assertEquals(300, entry1.getValue().longValue());
  }

  @Test
  public void testCheckDeletedState() {
    boolean hasException = false;
    try {
      deletionGroup1.checkDeletedState(5, 1);
    } catch (IllegalArgumentException e) {
      hasException = true;
    }
    assertTrue(hasException);

    assertEquals(NO_DELETED, deletionGroup1.checkDeletedState(1, 5));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(1, 10));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(2, 15));
    assertEquals(FULL_DELETED, deletionGroup1.checkDeletedState(10, 10));
    assertEquals(FULL_DELETED, deletionGroup1.checkDeletedState(10, 20));
    assertEquals(FULL_DELETED, deletionGroup1.checkDeletedState(30, 40));
    assertEquals(FULL_DELETED, deletionGroup1.checkDeletedState(10, 40));
    assertEquals(FULL_DELETED, deletionGroup1.checkDeletedState(40, 40));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(35, 45));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(40, 45));

    assertEquals(FULL_DELETED, deletionGroup1.checkDeletedState(50, 50));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(45, 50));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(50, 55));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(45, 55));

    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(5, 55));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(5, 500));

    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(120, 140));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(120, 150));

    assertEquals(NO_DELETED, deletionGroup1.checkDeletedState(201, 201));
    assertEquals(NO_DELETED, deletionGroup1.checkDeletedState(400, 500));

    assertEquals(NO_DELETED, deletionGroup1.checkDeletedState(201, 219));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(201, 220));
    assertEquals(FULL_DELETED, deletionGroup1.checkDeletedState(220, 220));
    assertEquals(FULL_DELETED, deletionGroup1.checkDeletedState(220, 230));
    assertEquals(FULL_DELETED, deletionGroup1.checkDeletedState(230, 250));
    assertEquals(FULL_DELETED, deletionGroup1.checkDeletedState(250, 300));
    assertEquals(FULL_DELETED, deletionGroup1.checkDeletedState(220, 300));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(220, 330));
    assertEquals(PARTIAL_DELETED, deletionGroup1.checkDeletedState(240, 350));

    // == test empty deletionGroup2
    assertEquals(NO_DELETED, deletionGroup2.checkDeletedState(1, 500));
  }

  @Test
  public void testIsDeleted() {

    assertEquals(false, deletionGroup1.isDeleted(5));
    assertEquals(true, deletionGroup1.isDeleted(10));
    assertEquals(true, deletionGroup1.isDeleted(20));
    assertEquals(true, deletionGroup1.isDeleted(40));
    assertEquals(false, deletionGroup1.isDeleted(45));

    assertEquals(true, deletionGroup1.isDeleted(50));

    assertEquals(false, deletionGroup1.isDeleted(60));
    assertEquals(true, deletionGroup1.isDeleted(70));
    assertEquals(true, deletionGroup1.isDeleted(100));
    assertEquals(true, deletionGroup1.isDeleted(120));
    assertEquals(false, deletionGroup1.isDeleted(122));

    assertEquals(true, deletionGroup1.isDeleted(220));
    assertEquals(true, deletionGroup1.isDeleted(250));
    assertEquals(true, deletionGroup1.isDeleted(300));
    assertEquals(false, deletionGroup1.isDeleted(400));

    // == test empty deletionGroup2
    assertEquals(false, deletionGroup2.isDeleted(100));
  }

  @Test
  public void testIsDeleted2() {
    DeletionGroup.IntervalCursor intervalCursor = new DeletionGroup.IntervalCursor();
    assertEquals(false, deletionGroup1.isDeleted(1, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(5, intervalCursor));
    assertEquals(true, deletionGroup1.isDeleted(10, intervalCursor));
    assertEquals(true, deletionGroup1.isDeleted(20, intervalCursor));
    assertEquals(true, deletionGroup1.isDeleted(40, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(45, intervalCursor));

    assertEquals(true, deletionGroup1.isDeleted(50, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(55, intervalCursor));

    assertEquals(true, deletionGroup1.isDeleted(70, intervalCursor));
    assertEquals(true, deletionGroup1.isDeleted(100, intervalCursor));
    assertEquals(true, deletionGroup1.isDeleted(120, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(125, intervalCursor));

    assertEquals(true, deletionGroup1.isDeleted(300, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(301, intervalCursor));

    intervalCursor = new DeletionGroup.IntervalCursor();
    assertEquals(true, deletionGroup1.isDeleted(10, intervalCursor));
    assertEquals(true, deletionGroup1.isDeleted(20, intervalCursor));
    assertEquals(true, deletionGroup1.isDeleted(40, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(45, intervalCursor));

    assertEquals(true, deletionGroup1.isDeleted(50, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(55, intervalCursor));

    assertEquals(true, deletionGroup1.isDeleted(70, intervalCursor));
    assertEquals(true, deletionGroup1.isDeleted(100, intervalCursor));
    assertEquals(true, deletionGroup1.isDeleted(120, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(125, intervalCursor));

    assertEquals(true, deletionGroup1.isDeleted(300, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(301, intervalCursor));

    intervalCursor.reset();
    assertEquals(true, deletionGroup1.isDeleted(50, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(55, intervalCursor));

    assertEquals(true, deletionGroup1.isDeleted(70, intervalCursor));
    assertEquals(true, deletionGroup1.isDeleted(100, intervalCursor));
    assertEquals(true, deletionGroup1.isDeleted(120, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(125, intervalCursor));

    assertEquals(true, deletionGroup1.isDeleted(300, intervalCursor));
    assertEquals(false, deletionGroup1.isDeleted(301, intervalCursor));

    intervalCursor.reset();
    assertEquals(false, deletionGroup1.isDeleted(301, intervalCursor));

    // == test empty deletionGroup2
    intervalCursor = new DeletionGroup.IntervalCursor();
    assertEquals(false, deletionGroup2.isDeleted(301, intervalCursor));
  }
}
