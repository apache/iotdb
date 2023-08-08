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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tools;

import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.Interval;
import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.ListTimeRangeImpl;

import org.junit.Assert;
import org.junit.Test;

public class ListTimeRangeImplTest {

  ListTimeRangeImpl listTimeRange = new ListTimeRangeImpl();

  @Test
  public void test01() {
    listTimeRange.addInterval(new Interval(30, 40));
    Assert.assertEquals(1, listTimeRange.getIntervalList().size());
    Assert.assertEquals(30, listTimeRange.getIntervalList().get(0).getStart());
    Assert.assertEquals(40, listTimeRange.getIntervalList().get(0).getEnd());
  }

  @Test
  public void test02() {
    listTimeRange.addInterval(new Interval(30, 40));
    listTimeRange.addInterval(new Interval(10, 20));
    listTimeRange.addInterval(new Interval(15, 20));
    listTimeRange.addInterval(new Interval(50, 60));
    Assert.assertEquals(3, listTimeRange.getIntervalList().size());
  }

  @Test
  public void test03() {
    listTimeRange.addInterval(new Interval(30, 40));
    listTimeRange.addInterval(new Interval(10, 20));
    listTimeRange.addInterval(new Interval(15, 20));
    listTimeRange.addInterval(new Interval(50, 60));
    listTimeRange.addInterval(new Interval(1, 100));
    Assert.assertEquals(1, listTimeRange.getIntervalList().size());
    Assert.assertEquals(1, listTimeRange.getIntervalList().get(0).getStart());
    Assert.assertEquals(100, listTimeRange.getIntervalList().get(0).getEnd());
  }

  @Test
  public void test04() {
    listTimeRange.addInterval(new Interval(30, 40));
    listTimeRange.addInterval(new Interval(10, 20));
    listTimeRange.addInterval(new Interval(15, 20));
    listTimeRange.addInterval(new Interval(50, 60));
    listTimeRange.addInterval(new Interval(5, 100));
    Assert.assertFalse(listTimeRange.isOverlapped(new Interval(1, 1)));
    Assert.assertFalse(listTimeRange.isOverlapped(new Interval(101, 103)));
  }

  @Test
  public void test05() {
    listTimeRange.addInterval(new Interval(30, 40));
    listTimeRange.addInterval(new Interval(10, 20));
    listTimeRange.addInterval(new Interval(20, 30));
    Assert.assertEquals(1, listTimeRange.getIntervalList().size());
  }

  @Test
  public void test06() {
    listTimeRange.addInterval(new Interval(1, 100));
    listTimeRange.addInterval(new Interval(1, 2000));
    Assert.assertEquals(1, listTimeRange.getIntervalList().size());
  }

  @Test
  public void test07() {
    listTimeRange.addInterval(new Interval(1, 10));
    listTimeRange.addInterval(new Interval(60, 70));
    listTimeRange.addInterval(new Interval(51, 55));
    Assert.assertEquals(51, listTimeRange.getIntervalList().get(1).getStart());
  }

  @Test
  public void testNoOverlap() {
    ListTimeRangeImpl listTimeRange = new ListTimeRangeImpl();
    listTimeRange.addInterval(new Interval(3, 5));
    Assert.assertFalse(listTimeRange.isOverlapped(new Interval(6, 10)));
    Assert.assertFalse(listTimeRange.isOverlapped(new Interval(1, 2)));
  }

  @Test
  public void testStartTimeOverlap() {
    ListTimeRangeImpl listTimeRange = new ListTimeRangeImpl();
    listTimeRange.addInterval(new Interval(1, 5));
    Assert.assertTrue(listTimeRange.isOverlapped(new Interval(4, 8)));
  }

  @Test
  public void testEndTimeOverlap() {
    ListTimeRangeImpl listTimeRange = new ListTimeRangeImpl();
    listTimeRange.addInterval(new Interval(1, 5));
    Assert.assertTrue(listTimeRange.isOverlapped(new Interval(0, 4)));
  }

  @Test
  public void testFullyOverlap() {
    ListTimeRangeImpl listTimeRange = new ListTimeRangeImpl();
    listTimeRange.addInterval(new Interval(2, 4));
    Assert.assertTrue(listTimeRange.isOverlapped(new Interval(1, 5)));
  }

  @Test
  public void testIntervalInsideCurrentInterval() {
    ListTimeRangeImpl listTimeRange = new ListTimeRangeImpl();
    listTimeRange.addInterval(new Interval(1, 5));
    Assert.assertTrue(listTimeRange.isOverlapped(new Interval(2, 4)));
  }

  @Test
  public void testBoundary() {
    ListTimeRangeImpl listTimeRange = new ListTimeRangeImpl();
    listTimeRange.addInterval(new Interval(3, 5));
    Assert.assertTrue(listTimeRange.isOverlapped(new Interval(1, 3)));
    Assert.assertTrue(listTimeRange.isOverlapped(new Interval(5, 6)));
  }
}
